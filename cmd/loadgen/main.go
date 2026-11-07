// Command loadgen drives the order service with open-loop traffic (SPEC 15) and
// prints a JSON report. Requests start on a fixed schedule whatever the response
// time, so a slow system does not reduce the offered load.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"dualwrite-study/internal/config"
	"dualwrite-study/internal/obs"
)

// report is the summary the runner records.
type report struct {
	Orders        int     `json:"orders"`
	Requests      int     `json:"requests"`
	Failed        int     `json:"failed"`
	ThroughputOps float64 `json:"throughput_ops"`
	LatencyP50MS  float64 `json:"latency_p50_ms"`
	LatencyP99MS  float64 `json:"latency_p99_ms"`
}

type generator struct {
	cfg    config.LoadGen
	client *http.Client

	mu        sync.Mutex
	latencies []float64
	requests  int
	failed    int
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadLoadGen()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	g := &generator{
		cfg: cfg,
		client: &http.Client{
			Timeout:   cfg.RequestTimeout,
			Transport: &http.Transport{MaxIdleConns: 2000, MaxIdleConnsPerHost: 2000},
		},
	}
	start := time.Now()
	orders := g.drive(ctx, start)
	return json.NewEncoder(os.Stdout).Encode(g.summary(orders, time.Since(start)))
}

// drive issues orders until the duration ends, then waits for their follow-ups.
// All random choices come from one seeded source, so a cycle can be replayed.
func (g *generator) drive(ctx context.Context, start time.Time) int {
	rng := rand.New(rand.NewPCG(g.cfg.Seed, g.cfg.Seed))
	var wg sync.WaitGroup
	orders := 0
	for elapsed := time.Duration(0); elapsed < g.cfg.Duration; elapsed += interval(g.cfg, elapsed) {
		select {
		case <-ctx.Done():
			wg.Wait()
			return orders
		case <-time.After(time.Until(start.Add(elapsed))):
		}
		action := followUp(rng.Float64())
		delay := time.Duration(50+rng.IntN(451)) * time.Millisecond
		orders++
		wg.Go(func() { g.order(action, delay) })
	}
	wg.Wait()
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
	if ok {
		g.latencies = append(g.latencies, float64(time.Since(began))/float64(time.Millisecond))
	} else {
		g.failed++
	}
	return payload, ok
}

func (g *generator) summary(orders int, elapsed time.Duration) report {
	g.mu.Lock()
	defer g.mu.Unlock()
	slices.Sort(g.latencies)
	return report{
		Orders:        orders,
		Requests:      g.requests,
		Failed:        g.failed,
		ThroughputOps: float64(len(g.latencies)) / elapsed.Seconds(),
		LatencyP50MS:  obs.Percentile(g.latencies, 50),
		LatencyP99MS:  obs.Percentile(g.latencies, 99),
	}
}
