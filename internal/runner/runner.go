package runner

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"dualwrite-study/internal/checker"
	"dualwrite-study/internal/config"
	"dualwrite-study/internal/load"
	"dualwrite-study/internal/obs"
)

const (
	orderService = "dualwrite-run-orderservice"
	serviceImage = "dualwrite/orderservice:dev"
	servicePort  = "8080"
	primary      = "dualwrite-postgres-primary-1"
	goMemory     = 256 << 20
)

// BaseContainers are the stack's long-running containers.
var BaseContainers = []string{
	primary, "dualwrite-postgres-replica-1", "dualwrite-broker-1-1", "dualwrite-kafka-connect-1", "dualwrite-toxiproxy-1",
}

// State timeouts (SPEC 14.2).
const (
	resetTimeout   = 60 * time.Second
	startTimeout   = 90 * time.Second
	faultTimeout   = 60 * time.Second
	recoverTimeout = 90 * time.Second
	checkTimeout   = 120 * time.Second
)

// ErrControlFailed stops the run: the setup is wrong, not the system under test.
var ErrControlFailed = errors.New("control assert failed")

// Options configure a run.
type Options struct {
	RunID, GitCommit, Host, Versions string
	Shard                            Shard
	Seed                             uint64
	ResultsDir                       string
}

// Runner executes cycles.
type Runner struct {
	opts        Options
	docker      *Docker
	results     *Results
	measurement config.Measurement
	toxics      *toxiproxy.Client
	admin       *kadm.Client
	baseLoad    config.LoadGen
}

// New creates a runner.
func New(opts Options, docker *Docker, results *Results) (*Runner, error) {
	measurement := config.LoadMeasurement()
	baseLoad, err := config.LoadLoadGen()
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(measurement.KafkaBrokers...))
	if err != nil {
		return nil, fmt.Errorf("kafka admin client: %w", err)
	}
	return &Runner{
		opts:        opts,
		docker:      docker,
		results:     results,
		measurement: measurement,
		toxics:      toxiproxy.NewClient("localhost:8474"),
		admin:       kadm.NewClient(client),
		baseLoad:    baseLoad,
	}, nil
}

// Close releases the runner's clients.
func (r *Runner) Close() { r.admin.Close() }

// Supported reports why this build cannot yet run the cycle, or nil. Refusing up
// front stops a vector from running with parts of it silently missing.
func Supported(c Cycle) error {
	switch {
	case c.Pattern != "P1":
		return fmt.Errorf("pattern %s is not built yet", c.Pattern)
	case len(c.Faults) > 0:
		return errors.New("fault injection is not built yet")
	case c.Stall != nil:
		return errors.New("stall points need the relay")
	case c.Crash != nil && c.Crash.Component != "orderservice":
		return fmt.Errorf("crash component %s is not built yet", c.Crash.Component)
	case len(c.Topology.Consumers) > 0:
		return errors.New("consumers are not built yet")
	case c.Crash == nil && c.Load.DurationS == 0:
		return errors.New("cycle has no fault and no load duration")
	}
	return nil
}

// minFreeDiskGB stops a run before the host disk can fill.
const minFreeDiskGB = 20

// Preflight refuses to start on a stack that would contaminate measurements.
// Recreating the broker wipes Connect's internal topics, after which Connect
// floods its log at about 20 MB/s and stalls everything sharing the disk.
func (r *Runner) Preflight(ctx context.Context) error {
	topics, err := r.admin.ListTopics(ctx, "connect-configs", "connect-offsets", "connect-status")
	if err != nil {
		return fmt.Errorf("list connect topics: %w", err)
	}
	for _, topic := range []string{"connect-configs", "connect-offsets", "connect-status"} {
		if detail, ok := topics[topic]; !ok || detail.Err != nil {
			return fmt.Errorf("topic %s is missing; recreate kafka-connect (docker compose rm -sfv kafka-connect && make up)", topic)
		}
	}
	var disk syscall.Statfs_t
	if err := syscall.Statfs(r.opts.ResultsDir, &disk); err != nil {
		return fmt.Errorf("read free disk: %w", err)
	}
	if free := disk.Bavail * uint64(disk.Bsize) >> 30; free < minFreeDiskGB {
		return fmt.Errorf("only %d GB free on disk, need %d", free, minFreeDiskGB)
	}
	return nil
}

// Run executes the cycles in order, skipping those already recorded. One bad
// cycle never stops the run; a failed control does.
func (r *Runner) Run(ctx context.Context, cycles []Cycle) error {
	for i, cycle := range cycles {
		if r.results.Done(cycle.ID) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		row := r.runCycle(ctx, cycle)
		if err := r.results.Append(row); err != nil {
			return err
		}
		slog.Info("cycle finished", "cycle", cycle.ID, "n", i+1, "of", len(cycles), "status", row.Status,
			"lost", row.Result.Lost, "hooked_lost", row.Result.HookedLost, "inflight", row.Result.InflightAtCrash,
			"cycle_s", row.FinishedAt.Sub(row.StartedAt).Seconds(), "load_s", row.LoadS)
		if row.Status == "CONTROL_FAIL" {
			return fmt.Errorf("%w: %s", ErrControlFailed, cycle.ID)
		}
	}
	return nil
}

// cycleRun holds one cycle's working state.
type cycleRun struct {
	cycle Cycle
	row   *Row
	logs  bytes.Buffer
	env   map[string]string
}

// failure carries the status a cycle ended with.
type failure struct {
	status string
	err    error
}

func (f *failure) Error() string { return f.status + ": " + f.err.Error() }

func failed(state string, err error) error { return &failure{"FAILED_" + state, err} }

func (r *Runner) runCycle(ctx context.Context, cycle Cycle) Row {
	seed := cycleSeed(r.opts.Seed, cycle.ID)
	sweep, _ := json.Marshal(cycle.Sweep)
	row := Row{
		RunID: r.opts.RunID, CycleID: cycle.ID, VectorID: cycle.VectorID, Pattern: cycle.Pattern, Mode: cycle.Mode,
		Repeat: cycle.Repeat, Seed: seed, SweepJSON: string(sweep), VersionsJSON: r.opts.Versions,
		LoadRate: cycle.Load.RatePerS, Host: r.opts.Host, Shard: r.opts.Shard.String(), GitCommit: r.opts.GitCommit,
		StartedAt: time.Now(),
	}
	run := &cycleRun{cycle: cycle, row: &row}
	var damage checker.Damage
	err := r.execute(ctx, run, seed, &damage)
	row.FinishedAt = time.Now()
	row.Status = "OK"
	var ended *failure
	if errors.As(err, &ended) {
		row.Status = ended.status
		slog.Error("cycle failed", "cycle", cycle.ID, "status", ended.status, "error", ended.err)
	} else if err != nil {
		row.Status = "FAILED_RUNNER"
		slog.Error("cycle failed", "cycle", cycle.ID, "error", err)
	}
	if err := r.keepEvidence(run, damage); err != nil {
		slog.Error("keeping evidence failed", "cycle", cycle.ID, "error", err)
	}
	return row
}

func (r *Runner) execute(ctx context.Context, run *cycleRun, seed uint64, damage *checker.Damage) error {
	cycle := run.cycle
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		_ = r.docker.Remove(cleanup, orderService)
	}()

	// RESET
	if err := r.reset(ctx, cycle); err != nil {
		return failed("RESET", err)
	}

	// START
	run.env = maps.Clone(cycle.Env)
	run.env["CYCLE_ID"] = cycle.ID
	run.env["PATTERN"] = cycle.Pattern
	if cycle.Crash != nil {
		afterN := cycle.Crash.AfterN
		if afterN == 0 {
			var err error
			if afterN, err = strconv.Atoi(cycle.Env["CRASH_AFTER_N"]); err != nil {
				return failed("START", fmt.Errorf("CRASH_AFTER_N: %w", err))
			}
		}
		rng := rand.New(rand.NewPCG(seed, seed+1))
		run.row.CrashAfterN = afterN + rng.IntN(afterN+1)
		run.row.CrashPoint = cycle.Crash.Point
		run.row.Fault = "crash_point"
		run.env["CRASH_AT"] = cycle.Crash.Point
		run.env["CRASH_AFTER_N"] = strconv.Itoa(run.row.CrashAfterN)
		run.env["ARM_ON_SIGNAL"] = "true"
	}
	if err := r.startService(ctx, run.env); err != nil {
		return failed("START", err)
	}

	// WARMUP
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	reports := make(chan loadOutcome, 1)
	loadConfig := r.baseLoad
	loadConfig.Mode, loadConfig.Rate, loadConfig.Seed = cycle.Load.Mode, cycle.Load.RatePerS, seed
	loadConfig.Duration = 0
	loadConfig.Warmup = time.Duration(cycle.Load.WarmupS) * time.Second
	cpuBefore, cpuErr := r.docker.CPU(ctx, primary)
	go func() {
		report, err := load.Run(loadCtx, loadConfig)
		reports <- loadOutcome{report, err}
	}()
	if err := sleep(ctx, loadConfig.Warmup); err != nil {
		return failed("WARMUP", err)
	}

	// FAULT and RECOVER
	if cycle.Crash != nil {
		if err := r.crashAndRecover(ctx, run); err != nil {
			stopLoad()
			<-reports
			return err
		}
		if err := sleep(ctx, time.Duration(cycle.Load.PostFaultS)*time.Second); err != nil {
			return failed("RECOVER", err)
		}
	} else if err := sleep(ctx, time.Duration(cycle.Load.DurationS)*time.Second); err != nil {
		return failed("FAULT", err)
	}

	// DRAIN
	stopLoad()
	outcome := <-reports
	if outcome.err != nil {
		return failed("DRAIN", outcome.err)
	}
	run.row.ThroughputOps, run.row.LoadS = outcome.report.ThroughputOps, outcome.report.DurationS
	run.row.LatencyP50MS, run.row.LatencyP99MS = outcome.report.LatencyP50MS, outcome.report.LatencyP99MS
	if cpuErr == nil {
		if cpuAfter, err := r.docker.CPU(ctx, primary); err == nil {
			run.row.DBCPUPct = cpuPercent(cpuBefore, cpuAfter)
		}
	}
	if err := r.drain(ctx, cycle); err != nil {
		return failed("DRAIN", err)
	}
	stop, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := r.docker.Stop(stop, orderService); err != nil {
		return failed("DRAIN", err)
	}
	if err := r.captureLogs(ctx, run); err != nil {
		return failed("DRAIN", err)
	}

	// CHECK
	checkCtx, cancelCheck := context.WithTimeout(ctx, checkTimeout)
	defer cancelCheck()
	in, err := checker.Collect(checkCtx, r.measurement.PostgresDSN, r.measurement.KafkaBrokers,
		"orders."+cycle.ID, bytes.NewReader(run.logs.Bytes()), cycle.Topology.Consumers)
	if err != nil {
		return failed("CHECK", err)
	}
	run.row.Result, *damage = checker.Check(in)
	if violation := assertionFailure(cycle, run.row.Result); violation != "" {
		return &failure{"CONTROL_FAIL", errors.New(violation)}
	}
	return nil
}

type loadOutcome struct {
	report load.Report
	err    error
}

// crashAndRecover arms the crash point now that warm-up is over, confirms the
// crash, and recreates the service with the point cleared (recovery rule).
func (r *Runner) crashAndRecover(ctx context.Context, run *cycleRun) error {
	faultCtx, cancel := context.WithTimeout(ctx, faultTimeout)
	defer cancel()
	if err := r.docker.Signal(faultCtx, orderService, "SIGUSR1"); err != nil {
		return failed("FAULT", err)
	}
	if err := r.docker.WaitForLog(faultCtx, orderService, obs.CrashPointHit); err != nil {
		_ = r.captureLogs(ctx, run)
		return &failure{"INVALID_NO_CRASH", err}
	}
	recoverCtx, cancelRecover := context.WithTimeout(ctx, recoverTimeout)
	defer cancelRecover()
	code, err := r.docker.WaitForExit(recoverCtx, orderService)
	if err != nil {
		return failed("RECOVER", err)
	}
	if code != 137 {
		return failed("RECOVER", fmt.Errorf("exit code %d, want 137 (SIGKILL)", code))
	}
	if err := r.captureLogs(recoverCtx, run); err != nil {
		return failed("RECOVER", err)
	}
	delete(run.env, "CRASH_AT")
	delete(run.env, "ARM_ON_SIGNAL")
	if err := r.startService(recoverCtx, run.env); err != nil {
		return failed("RECOVER", err)
	}
	return nil
}

func (r *Runner) captureLogs(ctx context.Context, run *cycleRun) error {
	logs, err := r.docker.Logs(ctx, orderService)
	if err != nil {
		return err
	}
	run.logs.Write(logs)
	return nil
}

// reset follows the order in SPEC 14.2. Connector steps arrive with P4 and P5.
func (r *Runner) reset(ctx context.Context, cycle Cycle) error {
	ctx, cancel := context.WithTimeout(ctx, resetTimeout)
	defer cancel()
	if err := r.docker.Remove(ctx, orderService); err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, r.measurement.PostgresDSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	_, err = conn.Exec(ctx, `TRUNCATE orders, orders_history, outbox, consumer.payments_naive,
		consumer.payments_dedup, consumer.processed_events, consumer.payments_upsert`)
	if err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	if err := r.createTopic(ctx, cycle); err != nil {
		return err
	}
	if err := r.toxics.ResetState(); err != nil {
		return fmt.Errorf("reset toxiproxy: %w", err)
	}
	return nil
}

func (r *Runner) createTopic(ctx context.Context, cycle Cycle) error {
	topic := "orders." + cycle.ID
	partitions, err := strconv.Atoi(cycle.Env["TOPIC_PARTITIONS"])
	if err != nil {
		return fmt.Errorf("TOPIC_PARTITIONS: %w", err)
	}
	replicas, err := strconv.Atoi(cycle.Env["TOPIC_RF"])
	if err != nil {
		return fmt.Errorf("TOPIC_RF: %w", err)
	}
	timestamps, minISR := "LogAppendTime", cycle.Env["TOPIC_MIN_ISR"]
	configs := map[string]*string{"message.timestamp.type": &timestamps, "min.insync.replicas": &minISR}

	// A topic left by an interrupted attempt at this cycle must not be reused.
	for {
		created, err := r.admin.CreateTopic(ctx, int32(partitions), int16(replicas), configs, topic)
		if err == nil && created.Err == nil {
			return nil
		}
		if err == nil {
			err = created.Err
		}
		// A briefly fenced broker reports no available brokers. RESET has a timeout
		// of its own, so wait within it rather than burn the cycle.
		if errors.Is(err, kerr.InvalidReplicationFactor) {
			if err := sleep(ctx, time.Second); err != nil {
				return fmt.Errorf("create topic %s: no broker available: %w", topic, err)
			}
			continue
		}
		if !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", topic, err)
		}
		if _, err := r.admin.DeleteTopic(ctx, topic); err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
			return fmt.Errorf("delete stale topic %s: %w", topic, err)
		}
		if err := sleep(ctx, time.Second); err != nil {
			return err
		}
	}
}

func (r *Runner) startService(ctx context.Context, env map[string]string) error {
	ctx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()
	spec := Spec{Name: orderService, Image: serviceImage, Env: env, Port: servicePort, MemoryBytes: goMemory}
	if err := r.docker.Recreate(ctx, spec); err != nil {
		return err
	}
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.baseLoad.Target+"/healthz", nil)
		if err != nil {
			return err
		}
		if response, err := http.DefaultClient.Do(request); err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		if err := sleep(ctx, 50*time.Millisecond); err != nil {
			return fmt.Errorf("service never became healthy: %w", err)
		}
	}
}

// drain waits until the topic's end offsets stop moving for quiet_s.
func (r *Runner) drain(ctx context.Context, cycle Cycle) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(cycle.Drain.TimeoutS)*time.Second)
	defer cancel()
	quiet := time.Duration(cycle.Drain.QuietS) * time.Second
	topic := "orders." + cycle.ID
	last, since := int64(-1), time.Now()
	for {
		offsets, err := r.admin.ListEndOffsets(ctx, topic)
		if err != nil {
			return fmt.Errorf("list end offsets: %w", err)
		}
		var total int64
		offsets.Each(func(o kadm.ListedOffset) { total += o.Offset })
		if total != last {
			last, since = total, time.Now()
		}
		if time.Since(since) >= quiet {
			return nil
		}
		if err := sleep(ctx, 500*time.Millisecond); err != nil {
			return err
		}
	}
}

// keepEvidence stores the damage list and logs of cycles worth explaining.
func (r *Runner) keepEvidence(run *cycleRun, damage checker.Damage) error {
	res := run.row.Result
	if run.row.Status == "OK" && res.Lost+res.Ghost+res.Duplicates+res.OutOfOrder == 0 {
		return nil
	}
	damageDir := filepath.Join(r.opts.ResultsDir, "damage")
	logDir := filepath.Join(r.opts.ResultsDir, "logs")
	if err := errors.Join(os.MkdirAll(damageDir, 0o755), os.MkdirAll(logDir, 0o755)); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(damage, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(damageDir, run.cycle.ID+".json"), raw, 0o644); err != nil {
		return err
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(run.logs.Bytes()); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(logDir, run.cycle.ID+".log.gz"), compressed.Bytes(), 0o644)
}

func cycleSeed(base uint64, cycleID string) uint64 {
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%d/%s", base, cycleID)
	return h.Sum64() >> 1 // fits a signed 64-bit reader of the CSV
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// Estimate is the planned work for one priority.
type Estimate struct {
	Cycles int
	LoadH  float64
}

// EstimateRun sums cycles and expected load time per priority.
func EstimateRun(cycles []Cycle) map[string]Estimate {
	estimates := make(map[string]Estimate)
	for _, c := range cycles {
		e := estimates[c.Priority]
		e.Cycles++
		e.LoadH += loadSeconds(c) / 3600
		estimates[c.Priority] = e
	}
	return estimates
}

func loadSeconds(c Cycle) float64 {
	seconds := float64(c.Load.WarmupS)
	if len(c.Faults) == 0 && c.Crash == nil && c.Stall == nil {
		return seconds + float64(c.Load.DurationS)
	}
	last := 0.0
	for _, f := range c.Faults {
		end := 0.0
		if f.AtS != nil {
			end = f.AtS.Mean()
		}
		if f.DurationS != nil {
			end += f.DurationS.Mean()
		}
		last = max(last, end)
	}
	return seconds + last + float64(c.Load.PostFaultS)
}

// FormatEstimates renders the table for -dry-run.
func FormatEstimates(estimates map[string]Estimate, overheadS float64) string {
	var out strings.Builder
	fmt.Fprintf(&out, "%-10s %7s %8s %8s\n", "priority", "cycles", "load h", "total h")
	var cycles int
	var hours float64
	for _, priority := range []string{"must", "should", "optional"} {
		e := estimates[priority]
		total := e.LoadH + float64(e.Cycles)*overheadS/3600
		fmt.Fprintf(&out, "%-10s %7d %8.1f %8.1f\n", priority, e.Cycles, e.LoadH, total)
		cycles += e.Cycles
		hours += total
	}
	fmt.Fprintf(&out, "%-10s %7d %8s %8.1f\n", "all", cycles, "", hours)
	return out.String()
}
