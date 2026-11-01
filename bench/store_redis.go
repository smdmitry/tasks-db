package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis: LIST на каждый бакет t:{hour:minute:shard}. Запись — RPUSH (+EXPIRE NX как страховка),
// чтение — деструктивный LPOP key count, пустой список Redis удаляет сам.
type redisStore struct {
	cfg Config
	rdb redis.UniversalClient
}

func newRedis(cfg Config) (*redisStore, error) {
	opts := &redis.UniversalOptions{
		Addrs:        []string{cfg.RedisAddr},
		PoolSize:     cfg.RedisPool,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	var rdb redis.UniversalClient
	if cfg.RedisCluster {
		rdb = redis.NewClusterClient(opts.Cluster())
	} else {
		rdb = redis.NewClient(opts.Simple())
	}
	return &redisStore{cfg: cfg, rdb: rdb}, nil
}

func redisKey(hour int64, minute, shard int16) string {
	// hash tag {...} — весь бакет в одном слоте Redis Cluster
	return "t:{" + strconv.FormatInt(hour, 10) + ":" + strconv.Itoa(int(minute)) + ":" + strconv.Itoa(int(shard)) + "}"
}

func (s *redisStore) Init(ctx context.Context, _ time.Time) error {
	return s.rdb.Ping(ctx).Err()
}

func (s *redisStore) Write(ctx context.Context, tasks []Task) error {
	groups := make(map[string][]any, len(tasks))
	for _, t := range tasks {
		k := redisKey(t.Hour, t.Minute, t.Shard)
		groups[k] = append(groups[k], t.Payload)
	}
	ttl := time.Duration(s.cfg.RedisTTL) * time.Second
	_, err := s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for k, vals := range groups {
			p.RPush(ctx, k, vals...)
			if ttl > 0 {
				p.ExpireNX(ctx, k, ttl)
			}
		}
		return nil
	})
	return err
}

func (s *redisStore) ReadPage(ctx context.Context, hour int64, minute, shard int16, _ any, limit int) (int, any, bool, error) {
	vals, err := s.rdb.LPopCount(ctx, redisKey(hour, minute, shard), limit).Result()
	if err == redis.Nil {
		return 0, nil, true, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	return len(vals), nil, len(vals) < limit, nil
}

func (s *redisStore) FinishBucket(context.Context, int64, int16, int16) error { return nil }
func (s *redisStore) FinishHour(context.Context, int64) error                 { return nil }
func (s *redisStore) Maintain(context.Context, time.Time) error               { return nil }

func (s *redisStore) Size(ctx context.Context) map[string]int64 {
	out := map[string]int64{}
	info, err := s.rdb.InfoMap(ctx, "memory", "keyspace").Result()
	if err != nil {
		return out
	}
	for _, sec := range info {
		if v, ok := sec["used_memory"]; ok {
			n, _ := strconv.ParseInt(v, 10, 64)
			out["redis_used_memory"] = n
		}
		if v, ok := sec["db0"]; ok {
			var keys int64
			fmt.Sscanf(v, "keys=%d", &keys)
			out["redis_keys"] = keys
		}
	}
	return out
}

func (s *redisStore) Close() { _ = s.rdb.Close() }
