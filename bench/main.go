package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "time/tzdata"

	"golang.org/x/time/rate"
)

type Bench struct {
	cfg     Config
	clock   *Clock
	store   Store
	planner *Planner

	written, writeCalls, writeErrs atomic.Int64
	read, readPages, readErrs      atomic.Int64
	bucketsDone                    atomic.Int64
	writeLat, readLat, bucketLat   *Lat

	workerMinute []atomic.Int64 // текущий (ещё не вычитанный) unix-minute каждого воркера
	hourMu       sync.Mutex
	hourDone     map[int64]int

	sub      Subscriber   // != nil — брокер сам доставляет задачи по таймеру (RocketMQ)
	subLagMs atomic.Int64 // максимальное опоздание доставки за окно отчёта, реальные мс

	prefilled   int64
	prefillRPS  float64
	prefillErrs int64
	started     time.Time
	stop        context.CancelFunc
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "report" {
		report(os.Args[2:])
		return
	}
	cfg := loadConfig()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	st, err := newStore(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	b := &Bench{
		cfg: cfg, store: st, planner: NewPlanner(cfg, nil),
		writeLat: NewLat(), readLat: NewLat(), bucketLat: NewLat(),
		workerMinute: make([]atomic.Int64, cfg.ConsumerWorkers),
		hourDone:     map[int64]int{},
	}
	// Брокеру с таймерами время исполнения уходит в реальных часах уже на prefill, поэтому часы
	// заводятся заранее: VirtualStart наступит через PREFILL_ETA. Бакетным хранилищам часы стартуют после prefill.
	if sub, ok := st.(Subscriber); ok {
		b.sub = sub
		if _, late := st.(interface{ ClockAfterPrefill() }); !late {
			b.clock = NewClockAt(time.Now().Add(cfg.PrefillETA), cfg.VirtualStart, cfg.Speed)
			sub.SetClock(b.clock)
		}
	}
	if cfg.Role != "consumer" {
		if err := st.Init(ctx, cfg.VirtualStart); err != nil {
			log.Fatal("init: ", err)
		}
		b.prefill(ctx)
	}
	if b.sub != nil && b.clock != nil {
		if d := time.Until(b.clock.RealAt(cfg.VirtualStart)); d > 0 {
			log.Printf("ждём старта часов %s", d.Round(time.Second))
			time.Sleep(d)
		} else if cfg.PrefillETA > 0 {
			log.Printf("WARN: prefill дольше PREFILL_ETA на %s, часы уже идут", (-d).Round(time.Second))
		}
	} else {
		b.clock = NewClock(cfg.VirtualStart, cfg.Speed)
		if b.sub != nil {
			b.sub.SetClock(b.clock)
		}
	}
	b.planner.clock = b.clock
	log.Printf("run=%s backend=%s role=%s virtual_start=%s speed=x%.0f duration=%s write_rps=%d batch=%d writers=%d shards=%d consumers=%d page=%d",
		cfg.RunID, cfg.Backend, cfg.Role, cfg.VirtualStart.Format("2006-01-02 15:04"), cfg.Speed, cfg.Duration,
		cfg.WriteRPS, cfg.WriteBatch, cfg.Writers, cfg.Shards, cfg.ConsumerWorkers, cfg.ReadPageLimit)

	runCtx, stop := context.WithTimeout(ctx, cfg.Duration)
	defer stop()
	b.stop = stop
	b.started = time.Now()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); b.maintain(runCtx) }()
	// drain: только prefill + консюмер, останавливаемся, когда всё вычитано
	if cfg.Role == "both" || cfg.Role == "producer" {
		b.startProducer(runCtx, &wg)
	}
	if cfg.Role != "producer" {
		if b.sub != nil {
			b.startSubscriber(runCtx)
		} else {
			b.startConsumer(runCtx, &wg)
		}
	}
	rows := b.reporter(runCtx)
	wg.Wait()
	b.summary(rows)
}

// prefill заливает очередь так, будто продюсер уже HORIZON_HOURS виртуальных часов писал с WRITE_RPS:
// иначе стационарная глубина очереди (и стационарное чтение) наступает только через сутки виртуального времени.
// Задачи, которые к старту уже были бы вычитаны, отбрасываются. Пишется батчами без лимита —
// заодно это замер максимальной скорости батчевой записи.
func (b *Bench) prefill(ctx context.Context) {
	total := int64(float64(b.cfg.WriteRPS) * float64(b.cfg.HorizonHours*3600) / b.cfg.Speed)
	if v := env("PREFILL", "auto"); v != "auto" {
		fmt.Sscan(v, &total)
	}
	if total <= 0 {
		return
	}
	startHour := b.cfg.VirtualStart.Unix() / 3600
	horizon := int64(b.cfg.HorizonHours) * 3600
	batch := envInt("PREFILL_BATCH", 500)
	var next, kept, errs atomic.Int64
	var wg sync.WaitGroup
	t0 := time.Now()
	for w := 0; w < b.cfg.Writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(t0.UnixNano()), uint64(w)+1000))
			tasks := make([]Task, 0, batch)
			hoursCache := map[int64][]int64{}
			for ctx.Err() == nil && next.Add(int64(batch)) <= total+int64(batch) {
				tasks = tasks[:0]
				for i := 0; i < batch; i++ {
					// момент записи — равномерно в [start-horizon, start)
					wh := (b.cfg.VirtualStart.Unix() - r.Int64N(horizon)) / 3600
					hours, ok := hoursCache[wh]
					if !ok {
						hours = b.planner.hoursFor(wh)
						hoursCache[wh] = hours
					}
					if len(hours) == 0 {
						continue
					}
					if t := b.planner.task(r, hours); t.Hour > startHour {
						tasks = append(tasks, t)
					}
				}
				if len(tasks) == 0 {
					continue
				}
				if err := b.store.Write(ctx, tasks); err != nil {
					if errs.Add(1) <= 3 {
						log.Print("prefill: ", err)
					}
					continue
				}
				if k := kept.Add(int64(len(tasks))); k/1_000_000 != (k-int64(len(tasks)))/1_000_000 {
					log.Printf("prefill: %d tasks, %.0f rows/s", k, float64(k)/time.Since(t0).Seconds())
				}
			}
		}(w)
	}
	wg.Wait()
	b.prefilled = kept.Load()
	b.prefillErrs = errs.Load()
	b.prefillRPS = float64(b.prefilled) / time.Since(t0).Seconds()
	log.Printf("prefill done: %d tasks (из %d сгенерированных), ошибок батчей %d, за %s, %.0f rows/s (batch=%d), size: %s",
		b.prefilled, total, b.prefillErrs, time.Since(t0).Round(time.Second), b.prefillRPS, batch, fmtSize(b.store.Size(ctx)))
}

func (b *Bench) maintain(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := b.store.Maintain(ctx, b.clock.Now()); err != nil && ctx.Err() == nil {
				log.Print("maintain: ", err)
			}
		}
	}
}

func (b *Bench) startProducer(ctx context.Context, wg *sync.WaitGroup) {
	var lim *rate.Limiter
	if b.cfg.WriteRPS > 0 {
		lim = rate.NewLimiter(rate.Limit(b.cfg.WriteRPS), max(b.cfg.WriteBatch, b.cfg.WriteRPS/10))
	}
	for w := 0; w < b.cfg.Writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(w)))
			tasks := make([]Task, b.cfg.WriteBatch)
			for ctx.Err() == nil {
				if lim != nil && lim.WaitN(ctx, b.cfg.WriteBatch) != nil {
					return
				}
				for i := range tasks {
					tasks[i] = b.planner.Next(r)
				}
				t0 := time.Now()
				err := b.store.Write(ctx, tasks)
				if err != nil {
					// драйвер может вернуть дедлайн раньше, чем ctx отменится на конце прогона
					if ctx.Err() == nil && !errors.Is(err, context.DeadlineExceeded) {
						b.writeErrs.Add(1)
						if b.writeErrs.Load()%1000 == 1 {
							log.Print("write: ", err)
						}
					}
					continue
				}
				b.writeLat.Record(time.Since(t0))
				b.writeCalls.Add(1)
				if n := b.written.Add(int64(len(tasks))); b.cfg.MaxWrites > 0 && n >= b.cfg.MaxWrites {
					b.stop() // лимит объёма: на больших payload минута записи не влезает в диск
				}
			}
		}(w)
	}
}

// Консюмер: воркер w обслуживает шарды s % W == w и идёт по минутным бакетам строго по порядку.
// Бакет минуты M вычитывается, когда виртуальное время прошло M+1 минуту (+grace).
// Вычитка страницами по READ_PAGE_LIMIT, опционально с лимитом READ_QPS. Прочитанное «отправляется в Kafka» = дропается.
func (b *Bench) startConsumer(ctx context.Context, wg *sync.WaitGroup) {
	var lim *rate.Limiter
	if b.cfg.ReadQPS > 0 {
		lim = rate.NewLimiter(rate.Limit(b.cfg.ReadQPS), b.cfg.ConsumerWorkers)
	}
	startMin := b.cfg.VirtualStart.Unix() / 60
	W := b.cfg.ConsumerWorkers
	for w := 0; w < W; w++ {
		b.workerMinute[w].Store(startMin)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for m := startMin; ctx.Err() == nil; m++ {
				b.workerMinute[w].Store(m)
				due := time.Unix((m+1)*60, 0).Add(b.cfg.ConsumerGrace)
				if d := b.clock.RealUntil(due); d > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(d):
					}
				}
				hour, minute := m/60, int16(m%60)
				for s := w; s < b.cfg.Shards; s += W {
					if !b.consumeBucket(ctx, lim, hour, minute, int16(s)) {
						return
					}
				}
				if minute == 59 {
					b.finishHour(ctx, hour)
				}
			}
		}(w)
	}
}

func (b *Bench) consumeBucket(ctx context.Context, lim *rate.Limiter, hour int64, minute, shard int16) bool {
	t0 := time.Now()
	var cursor any
	for {
		if lim != nil && lim.Wait(ctx) != nil {
			return false
		}
		q0 := time.Now()
		n, next, done, err := b.store.ReadPage(ctx, hour, minute, shard, cursor, b.cfg.ReadPageLimit)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			b.readErrs.Add(1)
			log.Printf("read h=%d m=%d s=%d: %v", hour, minute, shard, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		b.readLat.Record(time.Since(q0))
		b.readPages.Add(1)
		b.read.Add(int64(n)) // здесь был бы produce в Kafka
		if b.cfg.Role == "drain" && b.read.Load() >= b.prefilled {
			b.stop()
		}
		cursor = next
		if done {
			break
		}
	}
	if err := b.store.FinishBucket(ctx, hour, minute, shard); err != nil && ctx.Err() == nil {
		log.Print("finish bucket: ", err)
	}
	b.bucketLat.Record(time.Since(t0))
	b.bucketsDone.Add(1)
	return true
}

// startSubscriber — консюмер для брокера с таймерами: задачи приходят сами, когда наступило время.
// «Задержка чтения» здесь — опоздание доставки относительно заданного момента (в реальных мс).
func (b *Bench) startSubscriber(ctx context.Context) {
	err := b.sub.Subscribe(ctx, func(due []time.Time) {
		now := time.Now()
		for _, d := range due {
			late := max(0, now.Sub(d))
			b.readLat.Record(late)
			for {
				cur := b.subLagMs.Load()
				if late.Milliseconds() <= cur || b.subLagMs.CompareAndSwap(cur, late.Milliseconds()) {
					break
				}
			}
		}
		b.readPages.Add(1)
		b.read.Add(int64(len(due))) // здесь был бы produce в Kafka
		if b.cfg.Role == "drain" && b.read.Load() >= b.prefilled {
			b.stop()
		}
	})
	if err != nil {
		log.Fatal("subscribe: ", err)
	}
}

func (b *Bench) finishHour(ctx context.Context, hour int64) {
	b.hourMu.Lock()
	b.hourDone[hour]++
	last := b.hourDone[hour] == b.cfg.ConsumerWorkers
	if last {
		delete(b.hourDone, hour)
	}
	b.hourMu.Unlock()
	if last {
		if err := b.store.FinishHour(ctx, hour); err != nil && ctx.Err() == nil {
			log.Print("finish hour: ", err)
		}
	}
}

// lag — насколько (в виртуальных секундах) самый отстающий воркер позади момента, когда бакет стал доступен.
func (b *Bench) lag() float64 {
	if b.cfg.Role == "producer" {
		return 0
	}
	if b.sub != nil {
		return float64(b.subLagMs.Swap(0)) / 1000 * b.cfg.Speed
	}
	minM := int64(1 << 62)
	for i := range b.workerMinute {
		minM = min(minM, b.workerMinute[i].Load())
	}
	due := time.Unix((minM+1)*60, 0).Add(b.cfg.ConsumerGrace)
	return max(0, b.clock.Now().Sub(due).Seconds())
}

type Row struct {
	Elapsed      float64          `json:"elapsed_s"`
	Virtual      string           `json:"virtual_time"`
	WriteRPS     float64          `json:"write_rps"`
	WriteCallRPS float64          `json:"write_call_rps"`
	WriteLat     LatSnap          `json:"write_lat"`
	ReadRPS      float64          `json:"read_rps"`
	ReadQPS      float64          `json:"read_qps"`
	ReadLat      LatSnap          `json:"read_lat"`
	Backlog      int64            `json:"backlog"`
	LagVirtSec   float64          `json:"lag_virtual_s"`
	WriteErrs    int64            `json:"write_errs"`
	ReadErrs     int64            `json:"read_errs"`
	Size         map[string]int64 `json:"size"`
}

func (b *Bench) reporter(ctx context.Context) []Row {
	var rows []Row
	start := time.Now()
	t := time.NewTicker(b.cfg.ReportInterval)
	defer t.Stop()
	var pw, pc, pr, pp int64
	prev := start
	for {
		select {
		case <-ctx.Done():
			return rows
		case now := <-t.C:
			dt := now.Sub(prev).Seconds()
			prev = now
			w, c, r, p := b.written.Load(), b.writeCalls.Load(), b.read.Load(), b.readPages.Load()
			sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			row := Row{
				Elapsed:      now.Sub(start).Seconds(),
				Virtual:      b.clock.Now().In(b.cfg.Location).Format("01-02 15:04"),
				WriteRPS:     float64(w-pw) / dt,
				WriteCallRPS: float64(c-pc) / dt,
				WriteLat:     b.writeLat.Window(),
				ReadRPS:      float64(r-pr) / dt,
				ReadQPS:      float64(p-pp) / dt,
				ReadLat:      b.readLat.Window(),
				Backlog:      b.prefilled + w - r,
				LagVirtSec:   b.lag(),
				WriteErrs:    b.writeErrs.Load(),
				ReadErrs:     b.readErrs.Load(),
				Size:         b.store.Size(sctx),
			}
			cancel()
			pw, pc, pr, pp = w, c, r, p
			rows = append(rows, row)
			log.Printf("vt=%s | W %7.0f rows/s %6.0f calls/s p50=%.1fms p99=%.1fms err=%d | R %7.0f rows/s %6.0f q/s p50=%.1fms p99=%.1fms err=%d | backlog=%d lag=%.0fs(virt) | %s",
				row.Virtual, row.WriteRPS, row.WriteCallRPS, row.WriteLat.P50, row.WriteLat.P99, row.WriteErrs,
				row.ReadRPS, row.ReadQPS, row.ReadLat.P50, row.ReadLat.P99, row.ReadErrs,
				row.Backlog, row.LagVirtSec, fmtSize(row.Size))
		}
	}
}

func fmtSize(m map[string]int64) string {
	s := ""
	for _, k := range []string{"pg_db_bytes", "pg_wal_bytes", "pg_live_tup", "pg_dead_tup", "redis_used_memory", "redis_keys",
		"mongo_storage_bytes", "mongo_index_bytes", "mongo_docs", "tt_disk_bytes", "tt_mem_bytes", "tt_rows", "kafka_rows"} {
		if v, ok := m[k]; ok {
			if k == "pg_live_tup" || k == "pg_dead_tup" || k == "redis_keys" || k == "mongo_docs" || k == "tt_rows" || k == "kafka_rows" {
				s += fmt.Sprintf("%s=%d ", k, v)
			} else {
				s += fmt.Sprintf("%s=%.1fMB ", k, float64(v)/1e6)
			}
		}
	}
	return s
}

type Summary struct {
	RunID       string           `json:"run_id"`
	Backend     string           `json:"backend"`
	Variant     string           `json:"variant"`
	Config      Config           `json:"config"`
	Seconds     float64          `json:"seconds"`
	Prefilled   int64            `json:"prefilled"`
	PrefillRPS  float64          `json:"prefill_rps"`
	PrefillErrs int64            `json:"prefill_errs"`
	Written     int64            `json:"written"`
	Read        int64            `json:"read"`
	WriteRPS    float64          `json:"write_rps_avg"`
	ReadRPS     float64          `json:"read_rps_avg"`
	PeakReadRPS float64          `json:"read_rps_peak"`
	ReadQPS     float64          `json:"read_qps_avg"`
	WriteLat    LatSnap          `json:"write_lat"`
	ReadLat     LatSnap          `json:"read_lat"`
	BucketLat   LatSnap          `json:"bucket_lat"`
	MaxLag      float64          `json:"max_lag_virtual_s"`
	WriteErrs   int64            `json:"write_errs"`
	ReadErrs    int64            `json:"read_errs"`
	MaxSize     map[string]int64 `json:"max_size"`
	Rows        []Row            `json:"rows"`
}

func variant(c Config) string {
	v := fmt.Sprintf("batch=%d", c.WriteBatch)
	if c.Role == "drain" {
		v = fmt.Sprintf("DRAIN workers=%d page=%d", c.ConsumerWorkers, c.ReadPageLimit)
	}
	if c.PayloadMax > c.PayloadBytes {
		v += fmt.Sprintf(" payload=%d-%dB", c.PayloadBytes, c.PayloadMax)
	} else if c.PayloadRandom {
		v += " payload=random"
	}
	switch c.Backend {
	case "postgres":
		if c.WriteBatch > 1 {
			v += " " + c.PGWriteMode
		}
		v += " consume=" + c.PGConsumeMode
	case "scylla", "cassandra":
		v += " consume=" + c.ScyllaConsumeMode
	case "mongodb":
		v += " consume=" + c.MongoConsumeMode
	case "tarantool":
		v += " vinyl consume=" + c.TarantoolConsumeMode
	case "rabbitmq":
		v += fmt.Sprintf(" queue/bucket shards=%d", c.Shards)
	case "rocketmq":
		v += " timer-msg"
	case "kafka":
		v += fmt.Sprintf(" ring=%s partitions=%d", c.KafkaSlot, c.KafkaPartitions)
	}
	return v
}

func (b *Bench) summary(rows []Row) {
	sec := time.Since(b.started).Seconds()
	s := Summary{
		RunID: b.cfg.RunID, Backend: b.cfg.Backend, Variant: variant(b.cfg), Config: b.cfg, Seconds: sec,
		Prefilled: b.prefilled, PrefillRPS: b.prefillRPS, PrefillErrs: b.prefillErrs,
		Written: b.written.Load(), Read: b.read.Load(),
		WriteRPS: float64(b.written.Load()) / sec, ReadRPS: float64(b.read.Load()) / sec,
		ReadQPS:  float64(b.readPages.Load()) / sec,
		WriteLat: b.writeLat.Total(), ReadLat: b.readLat.Total(), BucketLat: b.bucketLat.Total(),
		WriteErrs: b.writeErrs.Load(), ReadErrs: b.readErrs.Load(),
		MaxSize: map[string]int64{}, Rows: rows,
	}
	for _, r := range rows {
		s.MaxLag = max(s.MaxLag, r.LagVirtSec)
		s.PeakReadRPS = max(s.PeakReadRPS, r.ReadRPS)
		for k, v := range r.Size {
			s.MaxSize[k] = max(s.MaxSize[k], v)
		}
	}
	log.Printf("SUMMARY %s [%s]: prefill=%d (%.0f/s) written=%d (%.0f/s, p99=%.1fms) read=%d (avg %.0f/s, peak %.0f/s, %.0f q/s, p99=%.1fms) max_lag=%.0fs(virt) errs w=%d r=%d size_max: %s",
		s.Backend, s.Variant, s.Prefilled, s.PrefillRPS, s.Written, s.WriteRPS, s.WriteLat.P99, s.Read, s.ReadRPS, s.PeakReadRPS, s.ReadQPS, s.ReadLat.P99,
		s.MaxLag, s.WriteErrs, s.ReadErrs, fmtSize(s.MaxSize))
	if err := os.MkdirAll(b.cfg.ResultsDir, 0o755); err != nil {
		log.Print(err)
		return
	}
	f := filepath.Join(b.cfg.ResultsDir, b.cfg.RunID+".json")
	data, _ := json.MarshalIndent(s, "", "  ")
	if err := os.WriteFile(f, data, 0o644); err != nil {
		log.Print(err)
		return
	}
	log.Print("saved ", f)
}
