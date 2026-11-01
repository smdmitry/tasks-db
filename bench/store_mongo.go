package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoDB (standalone, WiredTiger, w:1 без j — журнал сбрасывается раз в 100 мс, как commitlog у Scylla/Cassandra).
// Документ {_id: id, m, s, u, p}, keyset-чтение по индексу (m, s, _id). Консюмер:
//
//	drop   — коллекция на час h<hour> (аналог партиции PG), вычитанный час дропается целиком;
//	delete — одна коллекция tasks с полем h и индексом (h, m, s, _id), после вычитки бакета deleteMany по бакету.
type mongoStore struct {
	cfg     Config
	cli     *mongo.Client
	db      *mongo.Database
	mu      sync.Mutex
	created map[int64]bool
}

func newMongo(cfg Config) (*mongoStore, error) {
	opts := options.Client().ApplyURI(cfg.MongoURI).SetMaxPoolSize(uint64(cfg.MongoPool)).SetTimeout(30 * time.Second)
	cli, err := mongo.Connect(opts)
	if err != nil {
		return nil, err
	}
	return &mongoStore{cfg: cfg, cli: cli, db: cli.Database("bench"), created: map[int64]bool{}}, nil
}

func (s *mongoStore) coll(hour int64) *mongo.Collection {
	if s.cfg.MongoConsumeMode == "delete" {
		return s.db.Collection("tasks")
	}
	return s.db.Collection(fmt.Sprintf("h%d", hour))
}

func (s *mongoStore) Init(ctx context.Context, now time.Time) error {
	for s.cli.Ping(ctx, nil) != nil {
		time.Sleep(time.Second)
	}
	if err := s.db.Drop(ctx); err != nil {
		return err
	}
	if s.cfg.MongoConsumeMode == "delete" {
		_, err := s.db.Collection("tasks").Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{"h", 1}, {"m", 1}, {"s", 1}, {"_id", 1}}})
		return err
	}
	return s.Maintain(ctx, now)
}

// Maintain создаёт часовые коллекции с индексом наперёд (в режиме drop).
func (s *mongoStore) Maintain(ctx context.Context, now time.Time) error {
	if s.cfg.MongoConsumeMode == "delete" {
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
		if _, err := s.coll(h).Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{"m", 1}, {"s", 1}, {"_id", 1}}}); err != nil {
			return err
		}
		s.mu.Lock()
		s.created[h] = true
		s.mu.Unlock()
	}
	return nil
}

func (s *mongoStore) doc(t Task) bson.D {
	d := bson.D{{"_id", t.ID}, {"m", t.Minute}, {"s", t.Shard}, {"u", t.UserID}, {"p", t.Payload}}
	if s.cfg.MongoConsumeMode == "delete" {
		d = append(d, bson.E{"h", t.Hour})
	}
	return d
}

func (s *mongoStore) Write(ctx context.Context, tasks []Task) error {
	if len(tasks) == 1 {
		_, err := s.coll(tasks[0].Hour).InsertOne(ctx, s.doc(tasks[0]))
		return err
	}
	groups := map[int64][]any{}
	for _, t := range tasks {
		k := t.Hour
		if s.cfg.MongoConsumeMode == "delete" {
			k = 0
		}
		groups[k] = append(groups[k], s.doc(t))
	}
	for h, docs := range groups {
		if _, err := s.coll(h).InsertMany(ctx, docs, options.InsertMany().SetOrdered(false)); err != nil {
			return err
		}
	}
	return nil
}

func (s *mongoStore) bucketFilter(hour int64, minute, shard int16) bson.D {
	f := bson.D{{"m", minute}, {"s", shard}}
	if s.cfg.MongoConsumeMode == "delete" {
		f = append(bson.D{{"h", hour}}, f...)
	}
	return f
}

func (s *mongoStore) ReadPage(ctx context.Context, hour int64, minute, shard int16, cursor any, limit int) (int, any, bool, error) {
	f := s.bucketFilter(hour, minute, shard)
	if cursor != nil {
		f = append(f, bson.E{"_id", bson.D{{"$gt", cursor.(int64)}}})
	}
	cur, err := s.coll(hour).Find(ctx, f, options.Find().
		SetSort(bson.D{{"_id", 1}}).SetLimit(int64(limit)).SetProjection(bson.D{{"p", 1}}))
	if err != nil {
		return 0, nil, false, err
	}
	defer cur.Close(ctx)
	n := 0
	var last int64
	var doc struct {
		ID int64  `bson:"_id"`
		P  []byte `bson:"p"`
	}
	for cur.Next(ctx) {
		if err := cur.Decode(&doc); err != nil {
			return n, nil, false, err
		}
		last = doc.ID
		n++
	}
	return n, last, n < limit, cur.Err()
}

func (s *mongoStore) FinishBucket(ctx context.Context, hour int64, minute, shard int16) error {
	if s.cfg.MongoConsumeMode != "delete" {
		return nil
	}
	_, err := s.coll(hour).DeleteMany(ctx, s.bucketFilter(hour, minute, shard))
	return err
}

func (s *mongoStore) FinishHour(ctx context.Context, hour int64) error {
	if s.cfg.MongoConsumeMode == "delete" {
		return nil
	}
	return s.coll(hour).Drop(ctx)
}

func (s *mongoStore) Size(ctx context.Context) map[string]int64 {
	out := map[string]int64{}
	var st struct {
		StorageSize float64 `bson:"storageSize"`
		IndexSize   float64 `bson:"indexSize"`
		Objects     float64 `bson:"objects"`
	}
	if err := s.db.RunCommand(ctx, bson.D{{"dbStats", 1}}).Decode(&st); err == nil {
		out["mongo_storage_bytes"] = int64(st.StorageSize)
		out["mongo_index_bytes"] = int64(st.IndexSize)
		out["mongo_docs"] = int64(st.Objects)
	}
	return out
}

func (s *mongoStore) Close() { _ = s.cli.Disconnect(context.Background()) }
