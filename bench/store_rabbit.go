package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ: durable classic-очередь (v2) на каждый бакет q.<hour>.<minute>.<shard>, persistent-сообщения,
// publisher confirms (подтверждение после записи на диск). Очереди на будущие дневные часы объявляются заранее.
// Консюмер на прошедший бакет: узнаёт число сообщений (passive declare), читает страницами через
// basic.consume с prefetch=limit и ack multiple, вычитанную очередь удаляет. SHARDS для RabbitMQ меньше
// (очередей = часы окна × 60 × SHARDS, каждая — отдельный Erlang-процесс).
type rabbitStore struct {
	cfg      Config
	conns    []*amqp.Connection
	rr       atomic.Uint64
	pubPool  chan *amqp.Channel
	mu       sync.Mutex
	declared map[int64]bool
}

func newRabbit(cfg Config) (*rabbitStore, error) {
	s := &rabbitStore{cfg: cfg, pubPool: make(chan *amqp.Channel, cfg.Writers+cfg.ConsumerWorkers+8), declared: map[int64]bool{}}
	for i := 0; i < cfg.RabbitConns; i++ {
		var c *amqp.Connection
		var err error
		for try := 0; try < 60; try++ {
			if c, err = amqp.Dial(cfg.RabbitURL); err == nil {
				break
			}
			time.Sleep(time.Second)
		}
		if err != nil {
			return nil, err
		}
		s.conns = append(s.conns, c)
	}
	return s, nil
}

func rabbitQueue(hour int64, minute, shard int16) string {
	return fmt.Sprintf("q.%d.%d.%d", hour, minute, shard)
}

func (s *rabbitStore) inWindow(hour int64) bool {
	lh := time.Unix(hour*3600, 0).In(s.cfg.Location).Hour()
	return lh >= s.cfg.HourFrom && lh < s.cfg.HourTo
}

func (s *rabbitStore) newChannel(confirm bool) (*amqp.Channel, error) {
	ch, err := s.conns[s.rr.Add(1)%uint64(len(s.conns))].Channel()
	if err != nil {
		return nil, err
	}
	if confirm {
		if err := ch.Confirm(false); err != nil {
			ch.Close()
			return nil, err
		}
	}
	return ch, nil
}

func (s *rabbitStore) getPub() (*amqp.Channel, error) {
	select {
	case ch := <-s.pubPool:
		if !ch.IsClosed() {
			return ch, nil
		}
	default:
	}
	return s.newChannel(true)
}

func (s *rabbitStore) putPub(ch *amqp.Channel) {
	select {
	case s.pubPool <- ch:
	default:
		ch.Close()
	}
}

func (s *rabbitStore) Init(ctx context.Context, now time.Time) error {
	return s.Maintain(ctx, now)
}

var queueArgs = amqp.Table{"x-queue-type": "classic", "x-expires": int32(48 * 3600 * 1000)}

// Maintain объявляет очереди всех бакетов дневных часов в горизонте.
func (s *rabbitStore) Maintain(ctx context.Context, now time.Time) error {
	cur := now.Unix() / 3600
	var ch *amqp.Channel
	defer func() {
		if ch != nil {
			ch.Close()
		}
	}()
	for h := cur; h <= cur+int64(s.cfg.HorizonHours)+1; h++ {
		s.mu.Lock()
		done := s.declared[h]
		s.mu.Unlock()
		if done {
			continue
		}
		if s.inWindow(h) {
			if ch == nil {
				var err error
				if ch, err = s.newChannel(false); err != nil {
					return err
				}
			}
			for m := 0; m < 60; m++ {
				for sh := 0; sh < s.cfg.Shards; sh++ {
					if _, err := ch.QueueDeclare(rabbitQueue(h, int16(m), int16(sh)), true, false, false, false, queueArgs); err != nil {
						return err
					}
				}
			}
		}
		s.mu.Lock()
		s.declared[h] = true
		s.mu.Unlock()
	}
	return nil
}

func (s *rabbitStore) Write(ctx context.Context, tasks []Task) error {
	ch, err := s.getPub()
	if err != nil {
		return err
	}
	confirms := make([]*amqp.DeferredConfirmation, 0, len(tasks))
	for _, t := range tasks {
		dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", rabbitQueue(t.Hour, t.Minute, t.Shard), false, false,
			amqp.Publishing{DeliveryMode: amqp.Persistent, Body: t.Payload, MessageId: fmt.Sprint(t.ID)})
		if err != nil {
			ch.Close()
			return err
		}
		confirms = append(confirms, dc)
	}
	for _, dc := range confirms {
		ok, err := dc.WaitContext(ctx)
		if err != nil {
			ch.Close()
			return err
		}
		if !ok {
			s.putPub(ch)
			return errors.New("publish nacked")
		}
	}
	s.putPub(ch)
	return nil
}

type rabbitCursor struct {
	ch        *amqp.Channel
	queue     string
	del       <-chan amqp.Delivery
	remaining int
	broken    bool // канал закрыт ошибкой: неподтверждённые сообщения вернулись в очередь, открываемся заново
}

func (s *rabbitStore) ReadPage(ctx context.Context, hour int64, minute, shard int16, cursor any, limit int) (int, any, bool, error) {
	if !s.inWindow(hour) {
		return 0, nil, true, nil
	}
	c, _ := cursor.(*rabbitCursor)
	if c != nil && c.broken {
		c = nil
	}
	if c == nil {
		ch, err := s.newChannel(false)
		if err != nil {
			return 0, nil, false, err
		}
		name := rabbitQueue(hour, minute, shard)
		q, err := ch.QueueDeclarePassive(name, true, false, false, false, queueArgs)
		if err != nil {
			var ae *amqp.Error
			if errors.As(err, &ae) && ae.Code == amqp.NotFound { // очередь не создавалась (час до старта бенча)
				return 0, nil, true, nil
			}
			return 0, nil, false, err
		}
		c = &rabbitCursor{ch: ch, queue: name, remaining: q.Messages}
		if c.remaining > 0 {
			if err := ch.Qos(limit, 0, false); err != nil {
				ch.Close()
				return 0, nil, false, err
			}
			if c.del, err = ch.Consume(name, "", false, true, false, false, nil); err != nil {
				ch.Close()
				return 0, nil, false, err
			}
		}
	}
	want := min(limit, c.remaining)
	var last amqp.Delivery
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	for n := 0; n < want; n++ {
		select {
		case d, ok := <-c.del:
			if !ok {
				c.ch.Close()
				c.broken = true
				return n, nil, false, errors.New("consumer channel closed")
			}
			last = d
		case <-timeout.C:
			c.ch.Close()
			c.broken = true
			return n, nil, false, fmt.Errorf("%s: timeout, got %d of %d", c.queue, n, want)
		case <-ctx.Done():
			c.ch.Close()
			c.broken = true
			return n, nil, false, ctx.Err()
		}
	}
	if want > 0 {
		if err := last.Ack(true); err != nil {
			c.ch.Close()
			c.broken = true
			return 0, nil, false, err
		}
	}
	c.remaining -= want
	if c.remaining > 0 {
		return want, c, false, nil
	}
	// бакет вычитан: очередь больше не нужна
	_, err := c.ch.QueueDelete(c.queue, false, false, false)
	c.ch.Close()
	return want, nil, true, err
}

func (s *rabbitStore) FinishBucket(context.Context, int64, int16, int16) error { return nil }
func (s *rabbitStore) FinishHour(context.Context, int64) error                 { return nil }
func (s *rabbitStore) Size(context.Context) map[string]int64                   { return map[string]int64{} }

func (s *rabbitStore) Close() {
	for _, c := range s.conns {
		_ = c.Close()
	}
}
