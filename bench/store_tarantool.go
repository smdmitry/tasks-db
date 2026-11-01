package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tarantool/go-tarantool/v2"
)

// Tarantool, движок vinyl (LSM на диске). Логика в Lua-процедурах tarantool/init.lua, PK (hour, minute, shard, id).
// Консюмер:
//
//	drop   — спейс на час t<hour>, keyset-чтение без удаления, вычитанный час дропается;
//	delete — один спейс tasks, страница читается и удаляется в одной транзакции (take), как очередь.
type ttStore struct {
	cfg     Config
	conns   []*tarantool.Connection
	rr      atomic.Uint64
	mu      sync.Mutex
	created map[int64]bool
}

func newTarantool(cfg Config) (*ttStore, error) {
	s := &ttStore{cfg: cfg, created: map[int64]bool{}}
	for i := 0; i < cfg.TarantoolConns; i++ {
		var conn *tarantool.Connection
		var err error
		for try := 0; try < 60; try++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			conn, err = tarantool.Connect(ctx, tarantool.NetDialer{Address: cfg.TarantoolAddr},
				tarantool.Opts{Timeout: 30 * time.Second, Reconnect: time.Second, MaxReconnects: 30})
			cancel()
			if err == nil {
				break
			}
			time.Sleep(time.Second)
		}
		if err != nil {
			return nil, err
		}
		s.conns = append(s.conns, conn)
	}
	return s, nil
}

func (s *ttStore) conn() *tarantool.Connection {
	return s.conns[s.rr.Add(1)%uint64(len(s.conns))]
}

func (s *ttStore) call(ctx context.Context, fn string, args ...any) ([]any, error) {
	if args == nil {
		args = []any{} // nil ушёл бы в msgpack как nil, а не пустой массив
	}
	return s.conn().Do(tarantool.NewCallRequest(fn).Args(args).Context(ctx)).Get()
}

func (s *ttStore) space(hour int64) string {
	if s.cfg.TarantoolConsumeMode == "delete" {
		return "tasks"
	}
	return fmt.Sprintf("t%d", hour)
}

func (s *ttStore) Init(ctx context.Context, now time.Time) error {
	if _, err := s.call(ctx, "reset"); err != nil {
		return err
	}
	if s.cfg.TarantoolConsumeMode == "delete" {
		_, err := s.call(ctx, "ensure", "tasks")
		return err
	}
	return s.Maintain(ctx, now)
}

func (s *ttStore) Maintain(ctx context.Context, now time.Time) error {
	if s.cfg.TarantoolConsumeMode == "delete" {
		return nil
	}
	cur := now.Unix() / 3600
	for h := cur; h <= cur+int64(s.cfg.HorizonHours)+3; h++ {
		s.mu.Lock()
		done := s.created[h]
		s.mu.Unlock()
		if done {
			continue
		}
		if _, err := s.call(ctx, "ensure", s.space(h)); err != nil {
			return err
		}
		s.mu.Lock()
		s.created[h] = true
		s.mu.Unlock()
	}
	return nil
}

func ttTuple(t Task) []any {
	return []any{uint64(t.Hour), uint16(t.Minute), uint16(t.Shard), uint64(t.ID), uint64(t.UserID), t.Payload}
}

func (s *ttStore) Write(ctx context.Context, tasks []Task) error {
	if len(tasks) == 1 {
		_, err := s.call(ctx, "put", s.space(tasks[0].Hour), ttTuple(tasks[0]))
		return err
	}
	groups := map[string][]any{}
	for _, t := range tasks {
		name := s.space(t.Hour)
		groups[name] = append(groups[name], ttTuple(t))
	}
	_, err := s.call(ctx, "put_batch", groups)
	return err
}

func (s *ttStore) ReadPage(ctx context.Context, hour int64, minute, shard int16, cursor any, limit int) (int, any, bool, error) {
	var res []any
	var err error
	if s.cfg.TarantoolConsumeMode == "delete" {
		res, err = s.call(ctx, "take", s.space(hour), uint64(hour), uint16(minute), uint16(shard), limit)
	} else {
		last := uint64(0)
		if cursor != nil {
			last = cursor.(uint64)
		}
		res, err = s.call(ctx, "page", s.space(hour), uint64(hour), uint16(minute), uint16(shard), last, limit)
	}
	if err != nil {
		return 0, nil, false, err
	}
	rows, _ := res[0].([]any)
	n := len(rows)
	if n == 0 {
		return 0, cursor, true, nil
	}
	last := toUint64(rows[n-1].([]any)[0])
	return n, last, n < limit, nil
}

func toUint64(v any) uint64 {
	switch x := v.(type) {
	case uint64:
		return x
	case int64:
		return uint64(x)
	case uint32:
		return uint64(x)
	case int:
		return uint64(x)
	case uint:
		return uint64(x)
	case int8:
		return uint64(x)
	case uint8:
		return uint64(x)
	case int16:
		return uint64(x)
	case uint16:
		return uint64(x)
	case int32:
		return uint64(x)
	}
	panic(fmt.Sprintf("unexpected id type %T", v))
}

func (s *ttStore) FinishBucket(context.Context, int64, int16, int16) error { return nil }

func (s *ttStore) FinishHour(ctx context.Context, hour int64) error {
	if s.cfg.TarantoolConsumeMode == "delete" {
		return nil
	}
	_, err := s.call(ctx, "drop", s.space(hour))
	return err
}

func (s *ttStore) Size(ctx context.Context) map[string]int64 {
	out := map[string]int64{}
	res, err := s.call(ctx, "stats")
	if err != nil || len(res) == 0 {
		return out
	}
	if v, ok := res[0].([]any); ok && len(v) == 3 {
		out["tt_disk_bytes"] = int64(toUint64(v[0]))
		out["tt_mem_bytes"] = int64(toUint64(v[1]))
		out["tt_rows"] = int64(toUint64(v[2]))
	}
	return out
}

func (s *ttStore) Close() {
	for _, c := range s.conns {
		_ = c.Close()
	}
}
