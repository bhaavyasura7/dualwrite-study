// Package load generates open-loop order traffic (SPEC 15). Requests start on a
// fixed schedule whatever the response time, so a slow system does not reduce
// the offered load.
package load

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"dualwrite-study/internal/config"
	"dualwrite-study/internal/obs"
	"dualwrite-study/internal/raw"
)

// Report summarises a run. Latency and throughput exclude the warm-up.
type Report struct {
	Orders        int     `json:"orders"`
	Requests      int     `json:"requests"`
	Failed        int     `json:"failed"`
	ThroughputOps float64 `json:"throughput_ops"`
	LatencyP50MS  float64 `json:"latency_p50_ms"`
	LatencyP99MS  float64 `json:"latency_p99_ms"`
	DurationS     float64 `json:"duration_s"`
}

// sample is one request; failed ones are kept for service recovery (SPEC 11.2).
type sample struct {
	startMS   float64
	latencyMS float64
	ok        bool
}

type generator struct {
	cfg    config.LoadGen
	client *http.Client
	start  time.Time

	mu       sync.Mutex
	samples  []sample
	requests int
	failed   int
}

// Session is a load run in progress.
type Session struct {
	g      *generator
	report chan outcome
}

type outcome struct {
	report Report
	err    error
}

// Run drives the order service until the duration ends or ctx is cancelled, then
// waits for outstanding requests.
func Run(ctx context.Context, cfg config.LoadGen) (Report, error) {
	return Start(ctx, cfg).Wait()
}

// Start begins a run in the background.
func Start(ctx context.Context, cfg config.LoadGen) *Session {
	g := &generator{
		cfg: cfg,
		client: &http.Client{
			Timeout:   cfg.RequestTimeout,
			Transport: &http.Transport{MaxIdleConns: 2000, MaxIdleConnsPerHost: 2000},
		},
		start: time.Now(),
	}
	session := &Session{g: g, report: make(chan outcome, 1)}
	go func() {
		defer g.client.CloseIdleConnections()
		orders := g.drive(ctx)
		err := g.writeSeries()
		session.report <- outcome{g.summary(orders, time.Since(g.start)), err}
	}()
	return session
}

// Wait returns the report once the run has ended.
func (s *Session) Wait() (Report, error) {
	result := <-s.report
	return result.report, result.err
}

// Requests is every request so far, with its start time from Start, latency and
// outcome, in the order they completed.
func (s *Session) Requests() (time.Time, []raw.Request) {
	s.g.mu.Lock()
	defer s.g.mu.Unlock()
	out := make([]raw.Request, len(s.g.samples))
	for i, sm := range s.g.samples {
		out[i] = raw.Request{StartMS: sm.startMS, LatencyMS: sm.latencyMS, OK: sm.ok}
	}
	return s.g.start, out
}

// RecentP99 is the p99 latency of requests started within the last window.
func (s *Session) RecentP99(window time.Duration) float64 {
	s.g.mu.Lock()
	defer s.g.mu.Unlock()
	since := float64(time.Since(s.g.start)-window) / float64(time.Millisecond)
	var latencies []float64
	for i := len(s.g.samples) - 1; i >= 0 && s.g.samples[i].startMS >= since; i-- {
		if s.g.samples[i].ok {
			latencies = append(latencies, s.g.samples[i].latencyMS)
		}
	}
	slices.Sort(latencies)
	return obs.Percentile(latencies, 99)
}

// drive issues orders on schedule and waits for their follow-ups. All random
// choices come from one seeded source, so a cycle can be replayed.
func (g *generator) drive(ctx context.Context) int {
	rng := rand.New(rand.NewPCG(g.cfg.Seed, g.cfg.Seed))
	var wg sync.WaitGroup
	defer wg.Wait()
	orders := 0
	for elapsed := time.Duration(0); g.cfg.Duration == 0 || elapsed < g.cfg.Duration; elapsed += interval(g.cfg, elapsed) {
		select {
		case <-ctx.Done():
			return orders
		case <-time.After(time.Until(g.start.Add(elapsed))):
		}
		action := followUp(rng.Float64())
		delay := time.Duration(50+rng.IntN(451)) * time.Millisecond
		orders++
		wg.Go(func() { g.order(action, delay) })
	}
	return orders
}

// interval returns the gap to the next order at this point in the run.
func interval(cfg config.LoadGen, elapsed time.Duration) time.Duration {
	rate := cfg.Rate
	if cfg.Mode == "burst" && elapsed >= cfg.BurstAt && elapsed < cfg.BurstAt+cfg.BurstLength {
		rate = cfg.BurstRate
	}
	return time.Second / time.Duration(rate)
}

// followUp maps a uniform draw to the order's next step: 60% pay, 20% cancel,
// 20% nothing.
func followUp(draw float64) string {
	switch {
	case draw < 0.6:
		return "pay"
	case draw < 0.8:
		return "cancel"
	}
	return ""
}

func (g *generator) order(action string, delay time.Duration) {
	body, ok := g.post("/orders", []byte(`{"amount": 499.00}`))
	if !ok || action == "" {
		return
	}
	var created struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(body, &created); err != nil {
		return
	}
	time.Sleep(delay)
	g.post("/orders/"+created.OrderID+"/"+action, nil)
}

func (g *generator) post(path string, body []byte) ([]byte, bool) {
	began := time.Now()
	response, err := g.client.Post(g.cfg.Target+path, "application/json", bytes.NewReader(body))
	var payload []byte
	ok := false
	if err == nil {
		payload, err = io.ReadAll(response.Body)
		_ = response.Body.Close()
		ok = err == nil && response.StatusCode < 300
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.requests++
	g.samples = append(g.samples, sample{
		startMS:   float64(began.Sub(g.start)) / float64(time.Millisecond),
		latencyMS: float64(time.Since(began)) / float64(time.Millisecond),
		ok:        ok,
	})
	if !ok {
		g.failed++
	}
	return payload, ok
}

func (g *generator) writeSeries() error {
	if g.cfg.LatencyFile == "" {
		return nil
	}
	var out bytes.Buffer
	out.WriteString("start_ms,latency_ms\n")
	for _, s := range g.samples {
		if s.ok { // the file has always held successful requests only
			fmt.Fprintf(&out, "%.3f,%.3f\n", s.startMS, s.latencyMS)
		}
	}
	if err := os.WriteFile(g.cfg.LatencyFile, out.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write latency file: %w", err)
	}
	return nil
}

func (g *generator) summary(orders int, elapsed time.Duration) Report {
	g.mu.Lock()
	defer g.mu.Unlock()
	warmupMS := float64(g.cfg.Warmup) / float64(time.Millisecond)
	var latencies []float64
	for _, s := range g.samples {
		if s.ok && s.startMS >= warmupMS {
			latencies = append(latencies, s.latencyMS)
		}
	}
	slices.Sort(latencies)
	measured := max((elapsed - g.cfg.Warmup).Seconds(), 0.001)
	return Report{
		Orders:        orders,
		Requests:      g.requests,
		Failed:        g.failed,
		ThroughputOps: float64(len(latencies)) / measured,
		LatencyP50MS:  obs.Percentile(latencies, 50),
		LatencyP99MS:  obs.Percentile(latencies, 99),
		DurationS:     elapsed.Seconds(),
	}
}
