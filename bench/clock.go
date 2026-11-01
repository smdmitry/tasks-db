package main

import (
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// Clock — ускоренные виртуальные часы, чтобы прогнать суточный цикл за минуты.
type Clock struct {
	realStart time.Time
	virtStart time.Time
	speed     float64
}

func NewClock(virtStart time.Time, speed float64) *Clock {
	return NewClockAt(time.Now(), virtStart, speed)
}

// NewClockAt — часы, у которых VirtualStart наступает в реальный момент realStart (может быть в будущем).
func NewClockAt(realStart, virtStart time.Time, speed float64) *Clock {
	return &Clock{realStart: realStart, virtStart: virtStart, speed: speed}
}

// RealAt — реальный момент, когда наступит виртуальное время t.
func (c *Clock) RealAt(t time.Time) time.Time {
	return c.realStart.Add(time.Duration(float64(t.Sub(c.virtStart)) / c.speed))
}

func (c *Clock) Now() time.Time {
	el := time.Since(c.realStart)
	return c.virtStart.Add(time.Duration(float64(el) * c.speed))
}

// RealUntil — сколько реального времени ждать до виртуального момента t.
func (c *Clock) RealUntil(t time.Time) time.Duration {
	d := t.Sub(c.Now())
	if d <= 0 {
		return 0
	}
	return time.Duration(float64(d) / c.speed)
}

type Task struct {
	ID      int64
	UserID  int64
	Hour    int64 // unix hour (ts/3600) — часовая партиция
	Minute  int16 // 0..59 — минутный бакет внутри часа
	Shard   int16
	Payload []byte
}

// Due — виртуальный момент исполнения. Секунда внутри минуты берётся из ID: брокерам с таймерами
// (RocketMQ) нужно точное время, бакетным хранилищам хватает минуты.
func (t Task) Due() time.Time {
	return time.Unix(t.Hour*3600+int64(t.Minute)*60+t.ID%60, 0)
}

// Planner выбирает время исполнения: случайный будущий час из окна [HourFrom, HourTo)
// в пределах горизонта, внутри часа — случайная секунда.
type Planner struct {
	cfg      Config
	clock    *Clock
	payloads [][]byte
	seq      atomic.Int64
	cache    atomic.Pointer[hourCache]
}

type hourCache struct {
	curHour int64
	hours   []int64
}

// PAYLOAD_RANDOM=true (по умолчанию) — пул из 64k случайных payload'ов: несжимаемые данные,
// иначе Scylla/Cassandra (LZ4 на SSTable) сжимают повторяющийся payload почти в ноль.
func NewPlanner(cfg Config, clock *Clock) *Planner {
	p := &Planner{cfg: cfg, clock: clock}
	if cfg.PayloadMax > cfg.PayloadBytes {
		// payload переменного размера: срезы случайной длины из одного случайного буфера 64 MB
		r := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 43))
		buf := make([]byte, 64<<20)
		for i := range buf {
			buf[i] = byte(r.UintN(256))
		}
		for i := 0; i < 1<<16; i++ {
			size := cfg.PayloadBytes + r.IntN(cfg.PayloadMax-cfg.PayloadBytes+1)
			off := r.IntN(len(buf) - size)
			p.payloads = append(p.payloads, buf[off:off+size:off+size])
		}
		p.seq.Store(time.Now().UnixNano())
		return p
	}
	n := 1
	if cfg.PayloadRandom {
		n = 1 << 16
	}
	r := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 42))
	for i := 0; i < n; i++ {
		b := make([]byte, cfg.PayloadBytes)
		for j := range b {
			if n == 1 {
				b[j] = byte('a' + j%26)
			} else {
				b[j] = byte(r.UintN(256))
			}
		}
		p.payloads = append(p.payloads, b)
	}
	p.seq.Store(time.Now().UnixNano())
	return p
}

// hoursFor — часы, на которые можно запланировать задачу в момент cur (unix hour).
// Только строго будущие часы: задача не может попасть в уже прочитанный бакет.
func (p *Planner) hoursFor(cur int64) []int64 {
	var hours []int64
	for h := cur + 1; h <= cur+int64(p.cfg.HorizonHours); h++ {
		lh := time.Unix(h*3600, 0).In(p.cfg.Location).Hour()
		if lh >= p.cfg.HourFrom && lh < p.cfg.HourTo {
			hours = append(hours, h)
		}
	}
	return hours
}

func (p *Planner) eligibleHours() []int64 {
	cur := p.clock.Now().Unix() / 3600
	if c := p.cache.Load(); c != nil && c.curHour == cur {
		return c.hours
	}
	hours := p.hoursFor(cur)
	p.cache.Store(&hourCache{curHour: cur, hours: hours})
	return hours
}

func (p *Planner) Next(r *rand.Rand) Task {
	return p.task(r, p.eligibleHours())
}

func (p *Planner) task(r *rand.Rand, hours []int64) Task {
	h := hours[r.IntN(len(hours))]
	sec := r.IntN(3600)
	uid := r.Int64N(1_000_000_000)
	return Task{
		ID:      p.seq.Add(1),
		UserID:  uid,
		Hour:    h,
		Minute:  int16(sec / 60),
		Shard:   int16(uid % int64(p.cfg.Shards)),
		Payload: p.payloads[r.IntN(len(p.payloads))],
	}
}
