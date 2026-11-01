package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka без отдельного хранилища: кольцо топиков-слотов по KAFKA_SLOT (1h или 5m).
// Задача пишется в топик слота своего времени исполнения: ring-<(due / slot) % ring>.
// Кольцо длиннее горизонта на 2 слота, поэтому запись никогда не попадает в слот, который сейчас читается.
// Консюмер берёт слот целиком, когда он закончился (+CONSUMER_GRACE): читает все партиции до high watermark,
// затем DeleteRecords до него же — это аналог DROP партиции. Внутри слота порядка по времени нет,
// поэтому опоздание доставки — до длины слота (час или 5 минут).
type kafkaStore struct {
	cfg     Config
	clock   *Clock
	cl      *kgo.Client
	adm     *kadm.Client
	slot    int64 // секунд
	ring    int64
	topicMu []sync.Mutex
}

func newKafka(cfg Config) (*kafkaStore, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// payload случайный, остальные драйверы не жмут
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.MaxBufferedRecords(100_000),
	)
	if err != nil {
		return nil, err
	}
	slot := int64(cfg.KafkaSlot / time.Second)
	ring := (int64(cfg.HorizonHours)*3600+slot-1)/slot + 2
	return &kafkaStore{cfg: cfg, cl: cl, adm: kadm.NewClient(cl), slot: slot, ring: ring,
		topicMu: make([]sync.Mutex, ring)}, nil
}

func (s *kafkaStore) topic(i int64) string { return fmt.Sprintf("ring-%03d", i%s.ring) }

func (s *kafkaStore) topics() []string {
	ts := make([]string, s.ring)
	for i := range ts {
		ts[i] = s.topic(int64(i))
	}
	return ts
}

func (s *kafkaStore) SetClock(c *Clock) { s.clock = c }

// ClockAfterPrefill: в отличие от таймеров RocketMQ, Kafka не знает про реальное время исполнения,
// часы бенча можно заводить после prefill, как у бакетных хранилищ.
func (s *kafkaStore) ClockAfterPrefill() {}

func (s *kafkaStore) Init(ctx context.Context, _ time.Time) error {
	str := func(v string) *string { return &v }
	cfgs := map[string]*string{
		"retention.ms":    str("-1"), // удаляет консюмер через DeleteRecords
		"retention.bytes": str("-1"),
		"segment.bytes":   str(fmt.Sprint(s.cfg.KafkaSegmentBytes)),
	}
	var lastErr error
	for try := 0; try < 60; try++ {
		var r kadm.CreateTopicResponses
		r, lastErr = s.adm.CreateTopics(ctx, int32(s.cfg.KafkaPartitions), 1, cfgs, s.topics()...)
		if lastErr == nil {
			lastErr = r.Error()
		}
		if lastErr == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return lastErr
}

func (s *kafkaStore) record(t Task) *kgo.Record {
	due := make([]byte, 8)
	binary.BigEndian.PutUint64(due, uint64(t.Due().Unix()))
	return &kgo.Record{
		Topic:   s.topic(t.Due().Unix() / s.slot),
		Value:   t.Payload,
		Headers: []kgo.RecordHeader{{Key: "due", Value: due}},
	}
}

func (s *kafkaStore) Write(ctx context.Context, tasks []Task) error {
	recs := make([]*kgo.Record, len(tasks))
	for i, t := range tasks {
		recs[i] = s.record(t)
	}
	return s.cl.ProduceSync(ctx, recs...).FirstErr()
}

// Subscribe: диспетчер идёт по слотам по порядку и отдаёт закончившиеся слоты CONSUMER_WORKERS воркерам.
func (s *kafkaStore) Subscribe(ctx context.Context, handle func(due []time.Time)) error {
	slots := make(chan int64)
	go func() {
		defer close(slots)
		grace := s.cfg.ConsumerGrace
		for i := s.cfg.VirtualStart.Unix() / s.slot; ; i++ {
			end := time.Unix((i+1)*s.slot, 0).Add(grace)
			if d := time.Until(s.clock.RealAt(end)); d > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(d):
				}
			}
			select {
			case <-ctx.Done():
				return
			case slots <- i:
			}
		}
	}()
	for w := 0; w < s.cfg.ConsumerWorkers; w++ {
		go func() {
			for i := range slots {
				if err := s.consumeSlot(ctx, i, handle); err != nil && ctx.Err() == nil {
					log.Printf("kafka slot %s: %v", s.topic(i), err)
				}
			}
		}()
	}
	return nil
}

func (s *kafkaStore) consumeSlot(ctx context.Context, i int64, handle func(due []time.Time)) error {
	mu := &s.topicMu[i%s.ring]
	mu.Lock()
	defer mu.Unlock()
	topic := s.topic(i)
	starts, err := s.adm.ListStartOffsets(ctx, topic)
	if err == nil {
		err = starts.Error()
	}
	if err != nil {
		return err
	}
	ends, err := s.adm.ListEndOffsets(ctx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return err
	}
	from := map[int32]kgo.Offset{}
	left := map[int32]int64{} // сколько ещё прочитать до high watermark, зафиксированного на старте
	starts.Each(func(o kadm.ListedOffset) {
		if end := ends[topic][o.Partition].Offset; end > o.Offset {
			from[o.Partition] = kgo.NewOffset().At(o.Offset)
			left[o.Partition] = end
		}
	})
	if len(from) == 0 {
		return nil
	}
	cons, err := kgo.NewClient(
		kgo.SeedBrokers(s.cfg.KafkaBrokers),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: from}),
		kgo.FetchMaxPartitionBytes(8<<20),
		kgo.FetchMaxBytes(64<<20),
	)
	if err != nil {
		return err
	}
	defer cons.Close()
	for len(left) > 0 {
		fs := cons.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errs := fs.Errors(); len(errs) > 0 {
			return errs[0].Err
		}
		fs.EachPartition(func(p kgo.FetchTopicPartition) {
			end, ok := left[p.Partition]
			if !ok {
				return
			}
			due := make([]time.Time, 0, len(p.Records))
			for _, r := range p.Records {
				if r.Offset >= end {
					break
				}
				due = append(due, s.clock.RealAt(time.Unix(int64(binary.BigEndian.Uint64(r.Headers[0].Value)), 0)))
				if r.Offset == end-1 {
					delete(left, p.Partition)
				}
			}
			if len(due) > 0 {
				handle(due) // здесь был бы produce в целевой топик
			}
		})
	}
	// аналог DROP: сдвигаем log start offset, сегменты целиком ниже него брокер удаляет
	dr, err := s.adm.DeleteRecords(ctx, ends.Offsets())
	if err == nil {
		err = dr.Error()
	}
	return err
}

func (s *kafkaStore) Size(ctx context.Context) map[string]int64 {
	out := map[string]int64{}
	starts, err1 := s.adm.ListStartOffsets(ctx, s.topics()...)
	ends, err2 := s.adm.ListEndOffsets(ctx, s.topics()...)
	if err1 != nil || err2 != nil {
		return out
	}
	var rows int64
	ends.Each(func(o kadm.ListedOffset) {
		if st, ok := starts.Lookup(o.Topic, o.Partition); ok {
			rows += o.Offset - st.Offset
		}
	})
	out["kafka_rows"] = rows
	return out
}

func (s *kafkaStore) ReadPage(context.Context, int64, int16, int16, any, int) (int, any, bool, error) {
	panic("kafka: консюмер по слотам, ReadPage не используется")
}
func (s *kafkaStore) FinishBucket(context.Context, int64, int16, int16) error { return nil }
func (s *kafkaStore) FinishHour(context.Context, int64) error                 { return nil }
func (s *kafkaStore) Maintain(context.Context, time.Time) error               { return nil }
func (s *kafkaStore) Close()                                                  { s.cl.Close() }
