package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Backend string // scylla | cassandra | postgres | redis | mongodb | tarantool | rabbitmq | rocketmq | kafka
	Role    string // both | producer | consumer
	RunID   string

	Duration       time.Duration
	ReportInterval time.Duration
	ResultsDir     string

	// Виртуальные часы: 1 реальная секунда = Speed виртуальных секунд.
	Speed        float64
	VirtualStart time.Time
	Location     *time.Location
	HourFrom     int // окно планирования [HourFrom, HourTo)
	HourTo       int
	HorizonHours int

	// Запись
	WriteRPS      int // 0 = без лимита
	WriteBatch    int
	Writers       int
	PayloadBytes  int
	PayloadMax    int // >PayloadBytes — размер payload случайный в [PayloadBytes, PayloadMax]
	PayloadRandom bool
	Shards        int
	MaxWrites     int64 // 0 = без лимита; иначе прогон заканчивается, когда записано столько задач

	// Чтение
	ConsumerWorkers int
	ReadPageLimit   int
	ReadQPS         int // лимит запросов-страниц в секунду, 0 = без лимита
	ConsumerGrace   time.Duration

	ScyllaHosts       []string
	ScyllaConsistency string
	ScyllaConsumeMode string // ttl | delete
	ScyllaTTL         int

	PGDSN         string
	PGPoolSize    int
	PGWriteMode   string // copy | multi (для WriteBatch > 1)
	PGConsumeMode string // drop | delete

	RedisAddr    string
	RedisPool    int
	RedisTTL     int
	RedisCluster bool

	MongoURI         string
	MongoPool        int
	MongoConsumeMode string // drop | delete

	TarantoolAddr        string
	TarantoolConns       int
	TarantoolConsumeMode string // drop | delete

	RabbitURL   string
	RabbitConns int

	RocketNamesrv string
	RocketBroker  string
	RocketQueues  int
	PrefillETA    time.Duration // RocketMQ: сколько реального времени отвести на prefill до старта часов

	KafkaBrokers      string
	KafkaSlot         time.Duration // длина слота кольца топиков: 1h или 5m
	KafkaPartitions   int
	KafkaSegmentBytes int
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	v, err := strconv.Atoi(env(k, strconv.Itoa(def)))
	if err != nil {
		panic(fmt.Sprintf("bad %s: %v", k, err))
	}
	return v
}

func envDur(k string, def time.Duration) time.Duration {
	v, err := time.ParseDuration(env(k, def.String()))
	if err != nil {
		panic(fmt.Sprintf("bad %s: %v", k, err))
	}
	return v
}

func loadConfig() Config {
	loc, err := time.LoadLocation(env("TZ_NAME", "Europe/Moscow"))
	if err != nil {
		panic(err)
	}
	speed, err := strconv.ParseFloat(env("SPEED", "60"), 64)
	if err != nil || speed <= 0 {
		panic("bad SPEED")
	}

	c := Config{
		Backend:        env("BACKEND", "redis"),
		Role:           env("ROLE", "both"),
		Duration:       envDur("DURATION", 10*time.Minute),
		ReportInterval: envDur("REPORT_INTERVAL", 10*time.Second),
		ResultsDir:     env("RESULTS_DIR", "/results"),

		Speed:        speed,
		Location:     loc,
		HourFrom:     envInt("HOUR_FROM", 8),
		HourTo:       envInt("HOUR_TO", 22),
		HorizonHours: envInt("HORIZON_HOURS", 24),

		WriteRPS:      envInt("WRITE_RPS", 5000),
		WriteBatch:    envInt("WRITE_BATCH", 1),
		Writers:       envInt("WRITERS", 64),
		PayloadBytes:  envInt("PAYLOAD_BYTES", 300),
		PayloadMax:    envInt("PAYLOAD_BYTES_MAX", 0),
		PayloadRandom: env("PAYLOAD_RANDOM", "true") == "true",
		Shards:        envInt("SHARDS", 16),
		MaxWrites:     int64(envInt("MAX_WRITES", 0)),

		ConsumerWorkers: envInt("CONSUMER_WORKERS", 4),
		ReadPageLimit:   envInt("READ_PAGE_LIMIT", 1000),
		ReadQPS:         envInt("READ_QPS", 0),
		ConsumerGrace:   envDur("CONSUMER_GRACE", 5*time.Second),

		ScyllaHosts:       strings.Split(env("SCYLLA_HOSTS", "scylla"), ","),
		ScyllaConsistency: env("SCYLLA_CONSISTENCY", "QUORUM"),
		ScyllaConsumeMode: env("SCYLLA_CONSUME_MODE", "ttl"),
		ScyllaTTL:         envInt("SCYLLA_TTL", 48*3600),

		PGDSN:         env("PG_DSN", "postgres://bench:bench@postgres:5432/bench?sslmode=disable"),
		PGPoolSize:    envInt("PG_POOL", 64),
		PGWriteMode:   env("PG_WRITE_MODE", "copy"),
		PGConsumeMode: env("PG_CONSUME_MODE", "drop"),

		RedisAddr:    env("REDIS_ADDR", "redis:6379"),
		RedisPool:    envInt("REDIS_POOL", 64),
		RedisTTL:     envInt("REDIS_TTL", 48*3600),
		RedisCluster: env("REDIS_CLUSTER", "false") == "true",

		MongoURI:         env("MONGO_URI", "mongodb://mongodb:27017"),
		MongoPool:        envInt("MONGO_POOL", 64),
		MongoConsumeMode: env("MONGO_CONSUME_MODE", "drop"),

		TarantoolAddr:        env("TARANTOOL_ADDR", "tarantool:3301"),
		TarantoolConns:       envInt("TARANTOOL_CONNS", 8),
		TarantoolConsumeMode: env("TARANTOOL_CONSUME_MODE", "drop"),

		RabbitURL:   env("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/"),
		RabbitConns: envInt("RABBIT_CONNS", 8),

		RocketNamesrv: env("ROCKET_NAMESRV", "rocketmq-namesrv:9876"),
		RocketBroker:  env("ROCKET_BROKER", "rocketmq:10911"),
		RocketQueues:  envInt("ROCKET_QUEUES", 16),
		PrefillETA:    envDur("PREFILL_ETA", 3*time.Minute),

		KafkaBrokers:      env("KAFKA_BROKERS", "kafka:9092"),
		KafkaSlot:         envDur("KAFKA_SLOT", time.Hour),
		KafkaPartitions:   envInt("KAFKA_PARTITIONS", 4),
		KafkaSegmentBytes: envInt("KAFKA_SEGMENT_BYTES", 32<<20),
	}

	if vs := env("VIRTUAL_START", ""); vs != "" {
		t, err := time.ParseInLocation("2006-01-02T15:04", vs, loc)
		if err != nil {
			panic(fmt.Sprintf("bad VIRTUAL_START (want 2006-01-02T15:04): %v", err))
		}
		c.VirtualStart = t
	} else {
		// По умолчанию стартуем в 07:30 сегодняшнего дня: первые полчаса только запись,
		// дальше консюмер упирается в дневной пик 08-22.
		n := time.Now().In(loc)
		c.VirtualStart = time.Date(n.Year(), n.Month(), n.Day(), 7, 30, 0, 0, loc)
	}
	c.RunID = env("RUN_ID", fmt.Sprintf("%s-%s", c.Backend, time.Now().Format("20060102-150405")))
	if c.WriteBatch < 1 {
		c.WriteBatch = 1
	}
	return c
}
