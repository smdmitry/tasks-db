package main

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

// RocketMQ 5.x: таймер-сообщения с произвольным моментом доставки (свойство TIMER_DELIVER_MS, timer wheel брокера,
// точность 1 с). Часовые партиции и минутные бакеты здесь не нужны: брокер сам держит сообщение до момента
// исполнения и кладёт его в топик, консюмер просто подписан на топик (push consumer).
// Цена — двойная запись: сообщение пишется в commitlog в системный таймер-топик + timerlog,
// а при наступлении времени — ещё раз в commitlog уже в целевой топик.
type rocketStore struct {
	cfg   Config
	clock *Clock
	prod  rocketmq.Producer
	cons  rocketmq.PushConsumer
}

const rocketTopic = "tasks"

// resolveAddr: клиент RocketMQ принимает только ip:port, имена docker-сервисов резолвим сами.
func resolveAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	for try := 0; try < 60; try++ {
		if ips, err := net.LookupHost(host); err == nil && len(ips) > 0 {
			return net.JoinHostPort(ips[0], port)
		}
		time.Sleep(time.Second)
	}
	return addr
}

func newRocket(cfg Config) (*rocketStore, error) {
	rlog.SetLogLevel("error")
	cfg.RocketNamesrv = resolveAddr(cfg.RocketNamesrv)
	cfg.RocketBroker = resolveAddr(cfg.RocketBroker)
	p, err := rocketmq.NewProducer(
		producer.WithNameServer([]string{cfg.RocketNamesrv}),
		producer.WithGroupName("bench-producer"),
		producer.WithRetry(2),
		producer.WithSendMsgTimeout(10*time.Second),
		// клиент по умолчанию жмёт тело >4 KB zlib'ом; остальные драйверы не жмут — выключаем для равных условий
		producer.WithCompressMsgBodyOverHowmuch(4<<20),
	)
	if err != nil {
		return nil, err
	}
	if err := p.Start(); err != nil {
		return nil, err
	}
	return &rocketStore{cfg: cfg, prod: p}, nil
}

func (s *rocketStore) SetClock(c *Clock) { s.clock = c }

func (s *rocketStore) Init(ctx context.Context, _ time.Time) error {
	adm, err := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver([]string{s.cfg.RocketNamesrv})))
	if err != nil {
		return err
	}
	defer adm.Close()
	var lastErr error
	for try := 0; try < 60; try++ {
		lastErr = adm.CreateTopic(ctx, admin.WithTopicCreate(rocketTopic), admin.WithBrokerAddrCreate(s.cfg.RocketBroker),
			admin.WithReadQueueNums(s.cfg.RocketQueues), admin.WithWriteQueueNums(s.cfg.RocketQueues))
		if lastErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if lastErr != nil {
		return lastErr
	}
	// маршрут топика доходит до продюсера через namesrv не сразу: ждём, пока пройдёт пробное сообщение
	// (доставка через 2 суток — в прогон оно не попадёт)
	probe := primitive.NewMessage(rocketTopic, []byte("probe"))
	probe.WithProperty("TIMER_DELIVER_MS", strconv.FormatInt(time.Now().Add(48*time.Hour).UnixMilli(), 10))
	for try := 0; ; try++ {
		_, err := s.prod.SendSync(ctx, probe)
		if err == nil || try == 60 {
			return err
		}
		time.Sleep(time.Second)
	}
}

func (s *rocketStore) message(t Task) *primitive.Message {
	due := strconv.FormatInt(s.clock.RealAt(t.Due()).UnixMilli(), 10)
	m := primitive.NewMessage(rocketTopic, t.Payload)
	m.WithProperty("TIMER_DELIVER_MS", due)
	m.WithProperty("DUE_MS", due) // TIMER_DELIVER_MS брокер может переписать, своё свойство доезжает как есть
	m.WithKeys([]string{strconv.FormatInt(t.ID, 10)})
	return m
}

// Write: батчи таймер-сообщений брокер не принимает, поэтому сообщения батча отправляются параллельно,
// не больше rocketInflight одновременно (иначе брокер включает flow control "broker busy").
const rocketInflight = 32

func (s *rocketStore) Write(ctx context.Context, tasks []Task) error {
	if len(tasks) == 1 {
		_, err := s.prod.SendSync(ctx, s.message(tasks[0]))
		return err
	}
	// SendAsync в rocketmq-client-go v2.1.2 падает с nil pointer, если ответ не пришёл (таймаут под нагрузкой),
	// поэтому параллелим SendSync в горутинах
	errs := make(chan error, len(tasks))
	sem := make(chan struct{}, rocketInflight)
	for _, t := range tasks {
		sem <- struct{}{}
		go func(m *primitive.Message) {
			_, err := s.prod.SendSync(ctx, m)
			<-sem
			errs <- err
		}(s.message(t))
	}
	var first error
	for range tasks {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (s *rocketStore) Subscribe(ctx context.Context, handle func(due []time.Time)) error {
	c, err := rocketmq.NewPushConsumer(
		consumer.WithNameServer([]string{s.cfg.RocketNamesrv}),
		consumer.WithGroupName("bench-consumer"),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset),
		consumer.WithPullBatchSize(int32(min(s.cfg.ReadPageLimit, 1000))),
		consumer.WithConsumeMessageBatchMaxSize(s.cfg.ReadPageLimit),
		consumer.WithPullThresholdForQueue(int64(4*s.cfg.ReadPageLimit)),
		consumer.WithPullThresholdSizeForQueue(64),
		consumer.WithConsumeGoroutineNums(4*s.cfg.ConsumerWorkers),
	)
	if err != nil {
		return err
	}
	err = c.Subscribe(rocketTopic, consumer.MessageSelector{}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		due := make([]time.Time, len(msgs))
		for i, m := range msgs {
			ms, _ := strconv.ParseInt(m.GetProperty("DUE_MS"), 10, 64)
			due[i] = time.UnixMilli(ms)
		}
		handle(due) // здесь был бы produce в Kafka
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return err
	}
	if err := c.Start(); err != nil {
		return err
	}
	s.cons = c
	return nil
}

func (s *rocketStore) ReadPage(context.Context, int64, int16, int16, any, int) (int, any, bool, error) {
	panic("rocketmq: push consumer, ReadPage не используется")
}
func (s *rocketStore) FinishBucket(context.Context, int64, int16, int16) error { return nil }
func (s *rocketStore) FinishHour(context.Context, int64) error                 { return nil }
func (s *rocketStore) Maintain(context.Context, time.Time) error               { return nil }
func (s *rocketStore) Size(context.Context) map[string]int64                   { return map[string]int64{} }

func (s *rocketStore) Close() {
	if s.cons != nil {
		_ = s.cons.Shutdown()
	}
	_ = s.prod.Shutdown()
}
