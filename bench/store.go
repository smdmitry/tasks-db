package main

import (
	"context"
	"time"
)

// Store — бэкенд хранения задач: часовая партиция + минутный бакет (+ шард внутри бакета).
type Store interface {
	Init(ctx context.Context, now time.Time) error
	Write(ctx context.Context, tasks []Task) error
	// ReadPage читает до limit задач из бакета (hour, minute, shard) начиная с cursor.
	// done=true — бакет вычитан целиком.
	ReadPage(ctx context.Context, hour int64, minute, shard int16, cursor any, limit int) (n int, next any, done bool, err error)
	FinishBucket(ctx context.Context, hour int64, minute, shard int16) error
	FinishHour(ctx context.Context, hour int64) error
	// Maintain вызывается раз в секунду (создание партиций наперёд и т.п.).
	Maintain(ctx context.Context, now time.Time) error
	Size(ctx context.Context) map[string]int64
	Close()
}

// Subscriber — брокер сам доставляет задачи в момент исполнения (таймер-сообщения RocketMQ):
// вместо обхода бакетов консюмер подписывается на топик. Время исполнения брокеру передаётся
// в реальных часах, поэтому хранилищу нужен Clock.
type Subscriber interface {
	SetClock(c *Clock)
	// Subscribe не блокирует; handle вызывается на каждую доставленную пачку с реальными моментами исполнения.
	Subscribe(ctx context.Context, handle func(due []time.Time)) error
}

func newStore(cfg Config) (Store, error) {
	switch cfg.Backend {
	case "scylla", "cassandra": // один CQL-код, отличается только сервер
		return newScylla(cfg)
	case "postgres":
		return newPG(cfg)
	case "redis":
		return newRedis(cfg)
	case "mongodb":
		return newMongo(cfg)
	case "tarantool":
		return newTarantool(cfg)
	case "rabbitmq":
		return newRabbit(cfg)
	case "rocketmq":
		return newRocket(cfg)
	case "kafka":
		return newKafka(cfg)
	}
	panic("unknown BACKEND " + cfg.Backend)
}
