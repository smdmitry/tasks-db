package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

// ScyllaDB: партиция ((hour, minute, shard)), clustering id. Партиция = минутный бакет шарда,
// чтобы не получить large partition на весь час. Компакция TWCS + TTL.
// Консюмер:
//
//	ttl    — только читает, данные уходят по TTL (никаких лишних записей);
//	delete — после вычитки бакета пишет partition tombstone.
type scyllaStore struct {
	cfg  Config
	sess *gocql.Session
}

func newScylla(cfg Config) (*scyllaStore, error) {
	return &scyllaStore{cfg: cfg}, nil
}

func (s *scyllaStore) connect(keyspace string) (*gocql.Session, error) {
	cl := gocql.NewCluster(s.cfg.ScyllaHosts...)
	cl.Keyspace = keyspace
	cl.Consistency = gocql.ParseConsistency(s.cfg.ScyllaConsistency)
	cl.Timeout = 10 * time.Second
	cl.ConnectTimeout = 10 * time.Second
	cl.NumConns = 4
	cl.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	return cl.CreateSession()
}

func (s *scyllaStore) Init(ctx context.Context, _ time.Time) error {
	var sys *gocql.Session
	var err error
	for i := 0; i < 120; i++ {
		if sys, err = s.connect(""); err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return err
	}
	rf := env("SCYLLA_RF", "1")
	stmts := []string{
		`DROP KEYSPACE IF EXISTS bench`,
		fmt.Sprintf(`CREATE KEYSPACE bench WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': %s} AND tablets = {'enabled': false}`, rf),
		fmt.Sprintf(`CREATE TABLE bench.tasks (
			hour bigint, minute smallint, shard smallint, id bigint, user_id bigint, payload blob,
			PRIMARY KEY ((hour, minute, shard), id)
		) WITH default_time_to_live = %d
		  AND gc_grace_seconds = 3600
		  AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'HOURS', 'compaction_window_size': 1}`, s.cfg.ScyllaTTL),
	}
	for _, q := range stmts {
		if err := sys.Query(q).WithContext(ctx).Exec(); err != nil {
			// старые версии без tablets
			if strings.Contains(q, "tablets") {
				q = strings.Replace(q, " AND tablets = {'enabled': false}", "", 1)
				err = sys.Query(q).WithContext(ctx).Exec()
			}
			if err != nil {
				return fmt.Errorf("%s: %w", q, err)
			}
		}
	}
	sys.Close()
	s.sess, err = s.connect("bench")
	return err
}

const scyllaInsert = `INSERT INTO tasks (hour, minute, shard, id, user_id, payload) VALUES (?,?,?,?,?,?)`

func (s *scyllaStore) Write(ctx context.Context, tasks []Task) error {
	if len(tasks) == 1 {
		t := tasks[0]
		return s.sess.Query(scyllaInsert, t.Hour, t.Minute, t.Shard, t.ID, t.UserID, t.Payload).WithContext(ctx).Exec()
	}
	// Батч в Scylla эффективен только внутри одной партиции: группируем по бакету
	// и шлём по UNLOGGED BATCH на партицию параллельно.
	type pk struct {
		h    int64
		m, s int16
	}
	groups := map[pk][]Task{}
	for _, t := range tasks {
		k := pk{t.Hour, t.Minute, t.Shard}
		groups[k] = append(groups[k], t)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, g := range groups {
		wg.Add(1)
		go func(g []Task) {
			defer wg.Done()
			var err error
			if len(g) == 1 {
				t := g[0]
				err = s.sess.Query(scyllaInsert, t.Hour, t.Minute, t.Shard, t.ID, t.UserID, t.Payload).WithContext(ctx).Exec()
			} else {
				b := s.sess.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
				for _, t := range g {
					b.Query(scyllaInsert, t.Hour, t.Minute, t.Shard, t.ID, t.UserID, t.Payload)
				}
				err = s.sess.ExecuteBatch(b)
			}
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	return firstErr
}

func (s *scyllaStore) ReadPage(ctx context.Context, hour int64, minute, shard int16, cursor any, limit int) (int, any, bool, error) {
	var state []byte
	if cursor != nil {
		state = cursor.([]byte)
	}
	iter := s.sess.Query(`SELECT id, payload FROM tasks WHERE hour = ? AND minute = ? AND shard = ?`, hour, minute, shard).
		WithContext(ctx).PageSize(limit).PageState(state).Iter()
	n := 0
	var id int64
	var payload []byte
	for iter.Scan(&id, &payload) {
		n++
	}
	next := iter.PageState()
	if err := iter.Close(); err != nil {
		return n, nil, false, err
	}
	return n, next, len(next) == 0, nil
}

func (s *scyllaStore) FinishBucket(ctx context.Context, hour int64, minute, shard int16) error {
	if s.cfg.ScyllaConsumeMode != "delete" {
		return nil
	}
	return s.sess.Query(`DELETE FROM tasks WHERE hour = ? AND minute = ? AND shard = ?`, hour, minute, shard).WithContext(ctx).Exec()
}

func (s *scyllaStore) FinishHour(context.Context, int64) error   { return nil }
func (s *scyllaStore) Maintain(context.Context, time.Time) error { return nil }

// Size для Scylla снимает run.sh (du по data dir + nodetool), через CQL честно не посчитать.
func (s *scyllaStore) Size(context.Context) map[string]int64 { return map[string]int64{} }

func (s *scyllaStore) Close() {
	if s.sess != nil {
		s.sess.Close()
	}
}
