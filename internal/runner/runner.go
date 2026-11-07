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
	"os/exec"
	"path/filepath"
	"slices"
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
)

const (
	orderService     = "dualwrite-run-orderservice"
	relayName        = "dualwrite-run-relay-"
	serviceImage     = "dualwrite/orderservice:dev"
	relayImage       = "dualwrite/relay:dev"
	consumerName     = "dualwrite-run-consumer-"
	consumerImage    = "dualwrite/consumer:dev"
	maxRelays        = 4
	maxMembers       = 2
	servicePort      = "8080"
	primary          = "dualwrite-postgres-primary-1"
	replica          = "dualwrite-postgres-replica-1"
	broker           = "dualwrite-broker-1-1"
	connectContainer = "dualwrite-kafka-connect-1"
	pgProxy          = "dualwrite-toxiproxy-pg-1"
	goMemory         = 256 << 20
)

// BaseContainers are the stack's long-running containers.
var BaseContainers = []string{
	primary, "dualwrite-postgres-replica-1", "dualwrite-broker-1-1", "dualwrite-kafka-connect-1", "dualwrite-toxiproxy-1",
	pgProxy,
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
	Strict                           bool // final runs: enforce the pre-run checklist (SPEC 14.5)
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
	pgToxics    *toxiproxy.Client
	admin       *kadm.Client
	baseLoad    config.LoadGen
}

// New creates a runner.
func New(opts Options, docker *Docker, results *Results) (*Runner, error) {
	measurement, err := config.LoadMeasurement()
	if err != nil {
		return nil, fmt.Errorf("measurement config: %w", err)
	}
	baseLoad, err := config.LoadLoadGen()
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return &Runner{
		opts:        opts,
		docker:      docker,
		results:     results,
		measurement: measurement,
		toxics:      toxiproxy.NewClient("localhost:8474"),
		pgToxics:    toxiproxy.NewClient("localhost:8475"),
		baseLoad:    baseLoad,
	}, nil
}

// Close releases the runner's clients.
func (r *Runner) Close() {
	if r.admin != nil {
		r.admin.Close()
	}
}

// seeds lists the direct broker addresses of a topology. A closed port among the
// seeds would fail requests at random, so only running brokers are listed.
func (r *Runner) seeds(brokers int) []string {
	seeds := slices.Clone(r.measurement.KafkaBrokers)
	if brokers == 3 {
		seeds = append(seeds, "localhost:29092", "localhost:29093")
	}
	return seeds
}

// connectAdmin points the admin client at the running topology.
func (r *Runner) connectAdmin(brokers int) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(r.seeds(brokers)...))
	if err != nil {
		return fmt.Errorf("kafka admin client: %w", err)
	}
	r.Close()
	r.admin = kadm.NewClient(client)
	return nil
}

// Supported reports why this build cannot yet run the cycle, or nil. Refusing up
// front stops a vector from running with parts of it silently missing.
func Supported(c Cycle) error {
	switch {
	case c.Crash != nil && !slices.Contains([]string{"orderservice", "relay", "consumers"}, c.Crash.Component):
		return fmt.Errorf("crash component %s is not built yet", c.Crash.Component)
	case c.Crash != nil && c.Crash.Component == "relay" && c.Topology.Relays != 1:
		return errors.New("crash points on several relays are not built yet")
	case c.Crash == nil && c.Stall == nil && len(c.Faults) == 0 && c.Load.DurationS == 0:
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
	brokers, err := r.currentBrokers(ctx)
	if err != nil {
		return err
	}
	if err := r.connectAdmin(brokers); err != nil {
		return err
	}
	names := connectTopics(brokers)
	topics, err := r.admin.ListTopics(ctx, names...)
	if err != nil {
		return fmt.Errorf("list connect topics: %w", err)
	}
	for _, topic := range names {
		if detail, ok := topics[topic]; !ok || detail.Err != nil {
			return fmt.Errorf("topic %s is missing; recreate kafka-connect (docker compose rm -sfv kafka-connect && make up)", topic)
		}
	}
	if r.opts.Strict {
		if err := r.strictChecks(ctx); err != nil {
			return err
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
		if err := r.ensureBrokers(ctx, cycle.Topology.Brokers); err != nil {
			return fmt.Errorf("before %s: %w", cycle.ID, err)
		}
		row := r.runCycle(ctx, cycle)
		if err := r.results.Append(row); err != nil {
			return err
		}
		if cycle.RequiresRebuild {
			if err := r.rebuild(ctx); err != nil {
				return fmt.Errorf("after %s: %w", cycle.ID, err)
			}
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
	cycle      Cycle
	row        *Row
	logs       bytes.Buffer
	env        map[string]string
	pointEnv   map[string]string // crash or stall settings, applied only to their component
	components []Spec
	truthDSN   string    // the database that holds the truth; the replica after failover
	faultEnd   time.Time // when the last fault ended
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
	env, _ := json.Marshal(cycle.Env)
	row := Row{
		RunID: r.opts.RunID, CycleID: cycle.ID, VectorID: cycle.VectorID, Pattern: cycle.Pattern, Mode: cycle.Mode,
		Repeat: cycle.Repeat, Seed: seed, SweepJSON: string(sweep), EnvJSON: string(env), VersionsJSON: r.opts.Versions,
		LoadRate: cycle.Load.RatePerS, Host: r.opts.Host, Shard: r.opts.Shard.String(), GitCommit: r.opts.GitCommit,
		StartedAt: time.Now(),
	}
	run := &cycleRun{cycle: cycle, row: &row, truthDSN: r.measurement.PostgresDSN}
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
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_ = r.removeComponents(cleanup)
		_ = r.removeConnectors(cleanup)
	}()

	// RESET
	if err := r.reset(ctx, cycle); err != nil {
		return failed("RESET", err)
	}

	// START
	if err := r.prepare(run, seed); err != nil {
		return failed("START", err)
	}
	if err := r.startAll(ctx, run); err != nil {
		return failed("START", err)
	}
	stopSampling := r.sampleState(ctx, cycle)
	defer func() { run.row.WALRetainedMax, run.row.OutboxBacklogMax = stopSampling() }()

	// WARMUP
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	loadConfig := r.baseLoad
	loadConfig.Mode, loadConfig.Rate, loadConfig.Seed = cycle.Load.Mode, cycle.Load.RatePerS, seed
	loadConfig.Duration = 0
	loadConfig.BurstRate = cycle.Load.BurstRatePerS
	// Burst times share the clock of fault times: they start when warm-up ends.
	loadConfig.BurstAt = time.Duration(cycle.Load.WarmupS+cycle.Load.BurstAtS) * time.Second
	loadConfig.BurstLength = time.Duration(cycle.Load.BurstS) * time.Second
	loadConfig.Warmup = time.Duration(cycle.Load.WarmupS) * time.Second
	cpuBefore, cpuErr := r.docker.CPU(ctx, primary)
	session := load.Start(loadCtx, loadConfig)
	if err := sleep(ctx, max(loadConfig.Warmup-precheckWindow, 0)); err != nil {
		return failed("WARMUP", err)
	}
	if err := r.precheck(ctx, run, session); err != nil {
		return failed("WARMUP", err)
	}
	if run.row.PreP99MS == 0 {
		// No request succeeded before the fault: a fault on an idle system proves
		// nothing, and "lost 0" here would be false comfort.
		stopLoad()
		report, _ := session.Wait()
		run.row.LoadFailed = report.Failed
		return &failure{"INVALID_NO_LOAD", errors.New("no successful request before the fault")}
	}

	// FAULT and RECOVER
	if len(cycle.Faults) > 0 || cycle.Crash != nil || cycle.Stall != nil {
		if err := r.runFaults(ctx, run, seed); err != nil {
			stopLoad()
			_, _ = session.Wait()
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
	report, err := session.Wait()
	if err != nil {
		return failed("DRAIN", err)
	}
	run.row.ThroughputOps, run.row.LoadS, run.row.LoadFailed = report.ThroughputOps, report.DurationS, report.Failed
	run.row.LatencyP50MS, run.row.LatencyP99MS = report.LatencyP50MS, report.LatencyP99MS
	if cpuErr == nil {
		if cpuAfter, err := r.docker.CPU(ctx, primary); err == nil {
			run.row.DBCPUPct = cpuPercent(cpuBefore, cpuAfter)
		}
	}
	if err := r.drain(ctx, run); err != nil {
		return failed("DRAIN", err)
	}
	for _, component := range run.components {
		stop, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := r.docker.Stop(stop, component.Name)
		cancel()
		if err != nil {
			return failed("DRAIN", err)
		}
		if err := r.captureLogs(ctx, run, component.Name); err != nil {
			return failed("DRAIN", err)
		}
	}

	// CHECK
	checkCtx, cancelCheck := context.WithTimeout(ctx, checkTimeout)
	defer cancelCheck()
	in, err := checker.Collect(checkCtx, run.truthDSN, r.seeds(cycle.Topology.Brokers),
		"orders."+cycle.ID, bytes.NewReader(run.logs.Bytes()), cycle.Topology.Consumers)
	if err != nil {
		return failed("CHECK", err)
	}
	run.row.Result, *damage = checker.Check(in)
	if !run.faultEnd.IsZero() {
		if seconds, ok := checker.RecoveryS(in, run.faultEnd); ok {
			run.row.RecoveryS = strconv.FormatFloat(seconds, 'f', 3, 64)
		}
	}
	if violation := assertionFailure(cycle, run.row.Result); violation != "" {
		return &failure{"CONTROL_FAIL", errors.New(violation)}
	}
	return nil
}

// prepare works out the cycle's components and environment.
func (r *Runner) prepare(run *cycleRun, seed uint64) error {
	cycle := run.cycle
	run.env = maps.Clone(cycle.Env)
	run.env["CYCLE_ID"] = cycle.ID
	run.env["PATTERN"] = cycle.Pattern
	if cycle.Topology.Brokers == 3 {
		run.env["KAFKA_BROKERS"] = "toxiproxy:19091,toxiproxy:19092,toxiproxy:19093"
	}
	run.components = []Spec{{Name: orderService, Image: serviceImage, Port: servicePort, MemoryBytes: goMemory}}
	if cycle.Pattern == "P3" {
		for i := 1; i <= cycle.Topology.Relays; i++ {
			run.components = append(run.components, Spec{Name: relayName + strconv.Itoa(i), Image: relayImage, MemoryBytes: goMemory})
		}
	}
	for _, strategy := range cycle.Topology.Consumers {
		for member := 1; member <= cycle.Topology.ConsumersPerGroup; member++ {
			run.components = append(run.components, Spec{
				Name:        fmt.Sprintf("%s%s-%d", consumerName, strategy, member),
				Image:       consumerImage,
				Extra:       map[string]string{"CONSUMER_STRATEGY": strategy},
				MemoryBytes: goMemory,
			})
		}
	}
	point := cycle.Crash
	if point == nil {
		point = cycle.Stall
	}
	if point == nil {
		return nil
	}
	afterN := point.AfterN
	if afterN == 0 {
		var err error
		if afterN, err = strconv.Atoi(cycle.Env["CRASH_AFTER_N"]); err != nil {
			return fmt.Errorf("CRASH_AFTER_N: %w", err)
		}
	}
	rng := rand.New(rand.NewPCG(seed, seed+1))
	run.row.CrashAfterN = afterN + rng.IntN(afterN+1)
	run.row.CrashPoint = point.Point
	run.pointEnv = map[string]string{"ARM_ON_SIGNAL": "true"}
	if cycle.Crash != nil {
		run.pointEnv["CRASH_AT"] = point.Point
		run.pointEnv["CRASH_AFTER_N"] = strconv.Itoa(run.row.CrashAfterN)
	} else {
		run.pointEnv["STALL_AT"] = point.Point
		run.pointEnv["STALL_AFTER_N"] = strconv.Itoa(run.row.CrashAfterN)
		run.pointEnv["STALL_MS"] = strconv.Itoa(point.MS)
	}
	return nil
}

// pointTargets names the containers a crash or stall point runs in.
func pointTargets(run *cycleRun) []string {
	point := run.cycle.Crash
	if point == nil {
		point = run.cycle.Stall
	}
	switch {
	case point == nil:
		return nil
	case point.Component == "relay":
		return []string{relayName + "1"}
	case point.Component == "consumers":
		var names []string
		for _, component := range run.components {
			if strings.HasPrefix(component.Name, consumerName) {
				names = append(names, component.Name)
			}
		}
		return names
	}
	return []string{orderService}
}

// startAll creates the connector first, so its slot exists before any change.
func (r *Runner) startAll(ctx context.Context, run *cycleRun) error {
	ctx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()
	if isCDC(run.cycle.Pattern) {
		if err := r.createConnector(ctx, run.cycle, connector{}, r.measurement.PostgresDSN); err != nil {
			return err
		}
	}
	for _, component := range run.components {
		env := maps.Clone(run.env)
		if slices.Contains(pointTargets(run), component.Name) {
			maps.Copy(env, run.pointEnv)
		}
		if err := r.startComponent(ctx, component, env); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) startComponent(ctx context.Context, component Spec, env map[string]string) error {
	component.Env = maps.Clone(env)
	maps.Copy(component.Env, component.Extra)
	if err := r.docker.Recreate(ctx, component); err != nil {
		return err
	}
	if component.Port == "" {
		return nil
	}
	return r.waitHealthy(ctx)
}

const precheckWindow = 5 * time.Second

// precheck samples the machine and the system for the last 5 s before any fault,
// so a spike can later be traced to one or the other. It flags, never stops.
func (r *Runner) precheck(ctx context.Context, run *cycleRun, session *load.Session) error {
	before, cpuErr := r.docker.CPU(ctx, primary)
	if err := sleep(ctx, precheckWindow); err != nil {
		return err
	}
	if cpuErr == nil {
		if after, err := r.docker.CPU(ctx, primary); err == nil {
			run.row.PreDBCPUPct = cpuPercent(before, after)
		}
	}
	run.row.PreP99MS = session.RecentP99(precheckWindow)
	run.row.PreHostLoad = hostLoad()
	var flags []string
	if run.row.PreP99MS > r.measurement.PrecheckMaxP99MS {
		flags = append(flags, "latency")
	}
	if run.row.PreHostLoad > r.measurement.PrecheckMaxHostLoad {
		flags = append(flags, "load")
	}
	run.row.PreFlag = strings.Join(flags, "+")
	return nil
}

// hostLoad is the Mac's one-minute load average, or 0 if unavailable.
func hostLoad() float64 {
	out, err := exec.Command("sysctl", "-n", "vm.loadavg").Output()
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(out))
	if len(fields) < 2 {
		return 0
	}
	load, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return 0
	}
	return load
}

func (r *Runner) captureLogs(ctx context.Context, run *cycleRun, name string) error {
	logs, err := r.docker.Logs(ctx, name)
	if err != nil {
		return err
	}
	run.logs.Write(logs)
	return nil
}

func (r *Runner) removeComponents(ctx context.Context) error {
	names := []string{orderService}
	for i := 1; i <= maxRelays; i++ {
		names = append(names, relayName+strconv.Itoa(i))
	}
	for _, strategy := range []string{"naive", "dedup", "upsert"} {
		for member := 1; member <= maxMembers; member++ {
			names = append(names, fmt.Sprintf("%s%s-%d", consumerName, strategy, member))
		}
	}
	for _, name := range names {
		if err := r.docker.Remove(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

// reset follows the order in SPEC 14.2.
func (r *Runner) reset(ctx context.Context, cycle Cycle) error {
	ctx, cancel := context.WithTimeout(ctx, resetTimeout)
	defer cancel()
	if err := r.removeComponents(ctx); err != nil {
		return err
	}
	if err := r.removeConnectors(ctx); err != nil {
		return err
	}
	if isCDC(cycle.Pattern) {
		if err := r.ensureWorker(ctx, cycle); err != nil {
			return err
		}
	}
	conn, err := pgx.Connect(ctx, r.measurement.PostgresDSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	if err := setReplication(ctx, conn, cycle.Topology.Replication); err != nil {
		return err
	}
	if err := setIdleTimeout(ctx, conn, cycle.Env["PG_IDLE_TX_TIMEOUT_S"]); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, `TRUNCATE orders, orders_history, outbox, consumer.payments_naive,
		consumer.payments_dedup, consumer.processed_events, consumer.payments_upsert`)
	if err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	if err := r.createTopic(ctx, cycle); err != nil {
		return err
	}
	if err := errors.Join(r.toxics.ResetState(), r.pgToxics.ResetState()); err != nil {
		return fmt.Errorf("reset toxiproxy: %w", err)
	}
	return r.applyReplicationLatency(cycle)
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

func (r *Runner) waitHealthy(ctx context.Context) error {
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

// drain waits until the topic's end offsets and the unsent outbox count stop
// moving for quiet_s.
func (r *Runner) drain(ctx context.Context, run *cycleRun) error {
	cycle := run.cycle
	ctx, cancel := context.WithTimeout(ctx, time.Duration(cycle.Drain.TimeoutS)*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, run.truthDSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	quiet := time.Duration(cycle.Drain.QuietS) * time.Second
	topic := "orders." + cycle.ID
	var last [3]int64
	last[0] = -1
	since := time.Now()
	for {
		offsets, err := r.admin.ListEndOffsets(ctx, topic)
		if err != nil {
			return fmt.Errorf("list end offsets: %w", err)
		}
		var current [3]int64
		offsets.Each(func(o kadm.ListedOffset) { current[0] += o.Offset })
		if cycle.Pattern == "P3" {
			err := conn.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&current[1])
			if err != nil {
				return fmt.Errorf("count unsent: %w", err)
			}
		}
		if len(cycle.Topology.Consumers) > 0 {
			// Consumers may still be catching up after Kafka has gone quiet.
			err := conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM consumer.payments_naive)
				+ (SELECT count(*) FROM consumer.payments_dedup)
				+ (SELECT count(*) FROM consumer.payments_upsert)`).Scan(&current[2])
			if err != nil {
				return fmt.Errorf("count payments: %w", err)
			}
		}
		if current != last {
			last, since = current, time.Now()
		}
		if time.Since(since) >= quiet && current[1] == 0 {
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
