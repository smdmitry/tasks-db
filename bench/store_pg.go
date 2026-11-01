package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQL: RANGE-партиционирование по hour (одна партиция на час), индекс (minute, shard, id).
// Консюмер:
//
//	drop   — keyset-чтение страницами без удаления, вычитанный час отцепляется и дропается целиком (нет vacuum-churn);
//	delete — DELETE ... RETURNING страницами с FOR UPDATE SKIP LOCKED (классическая очередь, как у VK Знакомств).
type pgStore struct {
	cfg      Config
	pool     *pgxpool.Pool
	mu       sync.Mutex
	created  map[int64]bool
	startLSN string
}

func newPG(cfg Config) (*pgStore, error) {
	pc, err := pgxpool.ParseConfig(cfg.PGDSN)
	if err != nil {
		return nil, err
	}
	pc.MaxConns = int32(cfg.PGPoolSize)
	pool, err := pgxpool.NewWithConfig(context.Background(), pc)
	if err != nil {
		return nil, err
	}
	return &pgStore{cfg: cfg, pool: pool, created: map[int64]bool{}}, nil
}

func (s *pgStore) Init(ctx context.Context, now time.Time) error {
	for {
		if err := s.pool.Ping(ctx); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	stmts := []string{
		`DROP TABLE IF EXISTS tasks CASCADE`,
		`CREATE TABLE tasks (
			hour    bigint   NOT NULL,
			minute  smallint NOT NULL,
			shard   smallint NOT NULL,
			id      bigint   NOT NULL,
			user_id bigint   NOT NULL,
			payload bytea    NOT NULL
		) PARTITION BY RANGE (hour)`,
		`CREATE INDEX tasks_bucket_idx ON tasks (minute, shard, id)`,
	}
	for _, q := range stmts {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}
	// Удаляем партиции от прошлых запусков.
	rows, err := s.pool.Query(ctx, `SELECT tablename FROM pg_tables WHERE tablename LIKE 'tasks_h%'`)
	if err != nil {
		return err
	}
	old, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, t := range old {
		if _, err := s.pool.Exec(ctx, `DROP TABLE IF EXISTS `+t); err != nil {
			return err
		}
	}
	if err := s.pool.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&s.startLSN); err != nil {
		return err
	}
	return s.Maintain(ctx, now)
}

// Maintain создаёт партиции наперёд: CREATE + индекс + ATTACH (ATTACH берёт SHARE UPDATE EXCLUSIVE
// на родителя и не блокирует вставки, в отличие от CREATE TABLE ... PARTITION OF).
func (s *pgStore) Maintain(ctx context.Context, now time.Time) error {
	cur := now.Unix() / 3600
	for h := cur; h <= cur+int64(s.cfg.HorizonHours)+3; h++ {
		s.mu.Lock()
		done := s.created[h]
		s.mu.Unlock()
		if done {
			continue
		}
		name := fmt.Sprintf("tasks_h%d", h)
		stmts := []string{
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (LIKE tasks INCLUDING DEFAULTS)`, name),
			fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT %s_chk CHECK (hour >= %d AND hour < %d)`, name, name, h, h+1),
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_bucket_idx ON %s (minute, shard, id)`, name, name),
			fmt.Sprintf(`ALTER TABLE tasks ATTACH PARTITION %s FOR VALUES FROM (%d) TO (%d)`, name, h, h+1),
		}
		for _, q := range stmts {
			if _, err := s.pool.Exec(ctx, q); err != nil {
				return fmt.Errorf("%s: %w", q, err)
			}
		}
		s.mu.Lock()
		s.created[h] = true
		s.mu.Unlock()
	}
	return nil
}

var pgCols = []string{"hour", "minute", "shard", "id", "user_id", "payload"}

func (s *pgStore) Write(ctx context.Context, tasks []Task) error {
	if len(tasks) == 1 {
		t := tasks[0]
		_, err := s.pool.Exec(ctx,
			`INSERT INTO tasks (hour, minute, shard, id, user_id, payload) VALUES ($1,$2,$3,$4,$5,$6)`,
			t.Hour, t.Minute, t.Shard, t.ID, t.UserID, t.Payload)
		return err
	}
	if s.cfg.PGWriteMode == "copy" {
		_, err := s.pool.CopyFrom(ctx, pgx.Identifier{"tasks"}, pgCols,
			pgx.CopyFromSlice(len(tasks), func(i int) ([]any, error) {
				t := tasks[i]
				return []any{t.Hour, t.Minute, t.Shard, t.ID, t.UserID, t.Payload}, nil
			}))
		return err
	}
	// multi: один INSERT ... SELECT FROM unnest(массивов)
	n := len(tasks)
	hours, ids, uids := make([]int64, n), make([]int64, n), make([]int64, n)
	mins, shards := make([]int16, n), make([]int16, n)
	payloads := make([][]byte, n)
	for i, t := range tasks {
		hours[i], mins[i], shards[i], ids[i], uids[i], payloads[i] = t.Hour, t.Minute, t.Shard, t.ID, t.UserID, t.Payload
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO tasks (hour, minute, shard, id, user_id, payload)
		SELECT * FROM unnest($1::bigint[], $2::smallint[], $3::smallint[], $4::bigint[], $5::bigint[], $6::bytea[])`,
		hours, mins, shards, ids, uids, payloads)
	return err
}

func (s *pgStore) ReadPage(ctx context.Context, hour int64, minute, shard int16, cursor any, limit int) (int, any, bool, error) {
	if s.cfg.PGConsumeMode == "delete" {
		// minute/shard во внешнем предикате обязательны: иначе индекс (minute, shard, id) не используется
		// и каждый DELETE сканирует всю часовую партицию
		rows, err := s.pool.Query(ctx, `DELETE FROM tasks WHERE hour = $1 AND minute = $2 AND shard = $3 AND id IN (
				SELECT id FROM tasks WHERE hour = $1 AND minute = $2 AND shard = $3
				ORDER BY id LIMIT $4 FOR UPDATE SKIP LOCKED)
			RETURNING id, payload`, hour, minute, shard, limit)
		if err != nil {
			return 0, nil, false, err
		}
		n, err := drainRows(rows)
		return n, nil, n < limit, err
	}
	last := int64(-1 << 63)
	if cursor != nil {
		last = cursor.(int64)
	}
	rows, err := s.pool.Query(ctx, `SELECT id, payload FROM tasks
		WHERE hour = $1 AND minute = $2 AND shard = $3 AND id > $4 ORDER BY id LIMIT $5`,
		hour, minute, shard, last, limit)
	if err != nil {
		return 0, nil, false, err
	}
	n := 0
	var id int64
	var payload []byte
	for rows.Next() {
		if err := rows.Scan(&id, &payload); err != nil {
			rows.Close()
			return n, nil, false, err
		}
		last = id
		n++
	}
	rows.Close()
	return n, last, n < limit, rows.Err()
}

func drainRows(rows pgx.Rows) (int, error) {
	n := 0
	var id int64
	var payload []byte
	for rows.Next() {
		if err := rows.Scan(&id, &payload); err != nil {
			rows.Close()
			return n, err
		}
		n++
	}
	rows.Close()
	return n, rows.Err()
}

func (s *pgStore) FinishBucket(context.Context, int64, int16, int16) error { return nil }

// FinishHour — час вычитан: отцепляем партицию без блокировки вставок и дропаем.
func (s *pgStore) FinishHour(ctx context.Context, hour int64) error {
	name := fmt.Sprintf("tasks_h%d", hour)
	if _, err := s.pool.Exec(ctx, `ALTER TABLE tasks DETACH PARTITION `+name+` CONCURRENTLY`); err != nil &&
		!strings.Contains(err.Error(), "does not exist") && !strings.Contains(err.Error(), "is not a partition") {
		return err
	}
	_, err := s.pool.Exec(ctx, `DROP TABLE IF EXISTS `+name)
	return err
}

func (s *pgStore) Size(ctx context.Context) map[string]int64 {
	out := map[string]int64{}
	var db, wal, dead, live int64
	if err := s.pool.QueryRow(ctx, `SELECT pg_database_size(current_database()),
			pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::bigint`, s.startLSN).Scan(&db, &wal); err == nil {
		out["pg_db_bytes"] = db
		out["pg_wal_bytes"] = wal
	}
	if err := s.pool.QueryRow(ctx, `SELECT coalesce(sum(n_dead_tup),0)::bigint, coalesce(sum(n_live_tup),0)::bigint
			FROM pg_stat_user_tables WHERE relname LIKE 'tasks_h%'`).Scan(&dead, &live); err == nil {
		out["pg_dead_tup"] = dead
		out["pg_live_tup"] = live
	}
	return out
}

func (s *pgStore) Close() { s.pool.Close() }
