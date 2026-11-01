package main

import (
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Lat — потокобезопасная гистограмма латентности (микросекунды) + интервальная копия.
type Lat struct {
	mu    sync.Mutex
	total *hdrhistogram.Histogram
	win   *hdrhistogram.Histogram
}

func NewLat() *Lat {
	return &Lat{
		total: hdrhistogram.New(1, 60_000_000, 3),
		win:   hdrhistogram.New(1, 60_000_000, 3),
	}
}

func (l *Lat) Record(d time.Duration) {
	us := d.Microseconds()
	if us < 1 {
		us = 1
	}
	l.mu.Lock()
	_ = l.total.RecordValue(us)
	_ = l.win.RecordValue(us)
	l.mu.Unlock()
}

type LatSnap struct {
	Count int64   `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
	Max   float64 `json:"max_ms"`
}

func snap(h *hdrhistogram.Histogram) LatSnap {
	ms := func(v int64) float64 { return float64(v) / 1000 }
	return LatSnap{
		Count: h.TotalCount(),
		P50:   ms(h.ValueAtQuantile(50)),
		P95:   ms(h.ValueAtQuantile(95)),
		P99:   ms(h.ValueAtQuantile(99)),
		P999:  ms(h.ValueAtQuantile(99.9)),
		Max:   ms(h.Max()),
	}
}

func (l *Lat) Window() LatSnap {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := snap(l.win)
	l.win.Reset()
	return s
}

func (l *Lat) Total() LatSnap {
	l.mu.Lock()
	defer l.mu.Unlock()
	return snap(l.total)
}
