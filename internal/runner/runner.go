package runner

import (
	"bytes"
	"cmp"
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
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"dualwrite-study/internal/checker"
	"dualwrite-study/internal/config"
	"dualwrite-study/internal/dump"
	"dualwrite-study/internal/load"
	"dualwrite-study/internal/raw"
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
	Strict                           bool        // final runs: enforce the pre-run checklist (SPEC 14.5)
	FixedSeed                        bool        // trial only: every cycle uses Seed, to measure run-to-run noise
	StopRequested                    func() bool // true once the person asked to stop after this cycle
	StopFile                         string      // its presence asks the run to stop after the current cycle
	Seed                             uint64
	ResultsDir                       string
	// Backups run between cycles, never during one (SPEC 14.5).
	BackupEvery time.Duration
	Backup      func(context.Context) error
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

func (r *Runner) stopRequested() bool {
	if r.opts.StopRequested != nil && r.opts.StopRequested() {
		return true
	}
	_, err := os.Stat(r.opts.StopFile)
	return err == nil
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
	lastBackup := time.Now()
	defer func() {
		if ctx.Err() == nil {
			r.backup(context.WithoutCancel(ctx)) // the session's last rows
		}
	}()
	for i, cycle := range cycles {
		if r.opts.BackupEvery > 0 && time.Since(lastBackup) >= r.opts.BackupEvery {
			r.backup(ctx)
			lastBackup = time.Now()
		}
		if r.results.Done(cycle.ID) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.stopRequested() {
			slog.Info("stopped between cycles; rerun the same command to resume", "next", cycle.ID)
			return nil
		}
		if err := r.ensureBrokers(ctx, cycle.Topology.Brokers); err != nil {
			return fmt.Errorf("before %s: %w", cycle.ID, err)
		}
		row := r.runCycle(ctx, cycle)
		if ctx.Err() != nil {
			// Interrupted mid-cycle: not a result. It reruns on resume.
			slog.Warn("cycle interrupted and discarded", "cycle", cycle.ID)
			return ctx.Err()
		}
		row.Order = i + 1
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

// backup runs the backup between cycles and logs its time, which is excluded
// from every cycle's measurements. A failed backup is logged and the run goes
// on: the results stay on disk and the next backup includes them.
func (r *Runner) backup(ctx context.Context) {
	if r.opts.Backup == nil {
		return
	}
	began := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if err := r.opts.Backup(ctx); err != nil {
		slog.Error("backup failed; results are still on disk", "error", err, "backup_s", time.Since(began).Seconds())
		return
	}
	slog.Info("backup done", "backup_s", time.Since(began).Seconds())
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
	seed       uint64
	timeline   raw.Timeline
	faultMu    sync.Mutex // guards timeline.Faults, written by concurrent faults
	session    *load.Session
	damage     checker.Damage
	// Progress flags, so the end of the cycle knows what still needs doing.
	topicCreated bool // RESET created this cycle's topic
	dumped       bool // the topic's dump is written and verified
	rawWritten   bool // CHECK wrote the raw files
}

// state records a state's start and end in the timeline.
func (run *cycleRun) state(name string, start time.Time) {
	run.timeline.States[name] = [2]time.Time{start, time.Now()}
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
	if r.opts.FixedSeed {
		seed = r.opts.Seed
	}
	sweep, _ := json.Marshal(cycle.Sweep)
	env, _ := json.Marshal(cycle.Env)
	row := Row{
		RunID: r.opts.RunID, CycleID: cycle.ID, VectorID: cycle.VectorID, Pattern: cycle.Pattern, Mode: cycle.Mode,
		Repeat: cycle.Repeat, Seed: seed, SweepJSON: string(sweep), EnvJSON: string(env), VersionsJSON: r.opts.Versions,
		LoadRate: cycle.Load.RatePerS, Host: r.opts.Host, Shard: r.opts.Shard.String(), GitCommit: r.opts.GitCommit,
		TruthSource: "primary", Consumers: len(cycle.Topology.Consumers) > 0,
		StartedAt: time.Now(),
	}
	run := &cycleRun{cycle: cycle, row: &row, truthDSN: r.measurement.PostgresDSN, seed: seed}
	run.timeline = raw.Timeline{
		CycleID: cycle.ID, Pattern: cycle.Pattern, Seed: seed, Env: cycle.Env,
		Consumers: cycle.Topology.Consumers, States: make(map[string][2]time.Time),
	}
	err := r.execute(ctx, run, seed)
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
	if ctx.Err() == nil {
		r.finishCycle(ctx, run)
	}
	if err := r.keepEvidence(run); err != nil {
		slog.Error("keeping evidence failed", "cycle", cycle.ID, "error", err)
	}
	return row
}

// finishCycle keeps a failed cycle's evidence and leaves the broker clean: a
// topic created but not yet dumped is dumped now, a verified topic is deleted,
// raw files not yet written are written as far as they can be, and the
// timeline is always written last (SPEC 11.5, 14.2).
func (r *Runner) finishCycle(ctx context.Context, run *cycleRun) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), readyCap+dump.Timeout+deleteWait)
	defer cancel()
	if run.topicCreated && !run.dumped {
		if err := r.dumpCycle(cleanup, run); err != nil {
			slog.Error("dump of failed cycle failed; topic kept", "cycle", run.cycle.ID, "error", err)
		}
	}
	if run.topicCreated {
		if err := r.deleteCycleTopics(cleanup, run); err != nil {
			slog.Warn("delete cycle topics", "cycle", run.cycle.ID, "error", err)
		}
	}
	if !run.rawWritten && run.topicCreated {
		if err := r.writeRaw(cleanup, run); err != nil {
			slog.Warn("raw files of failed cycle incomplete", "cycle", run.cycle.ID, "error", err)
		}
	}
	if err := raw.WriteJSON(raw.Dir(r.opts.ResultsDir, run.cycle.ID), raw.TimelineFile, run.timeline); err != nil {
		slog.Error("write timeline", "cycle", run.cycle.ID, "error", err)
	}
}

func (r *Runner) execute(ctx context.Context, run *cycleRun, seed uint64) error {
	cycle := run.cycle
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_ = r.removeComponents(cleanup)
		_ = r.removeConnectors(cleanup)
	}()

	// RESET. The broker must be serving first: a broker restarted by the last
	// cycle may still be reloading its logs.
	began := time.Now()
	waited, err := r.waitReady(ctx, cycle.Topology.Brokers, "", "RESET")
	run.row.BrokerWaitS += waited.Seconds()
	if err != nil {
		return failed("RESET", err)
	}
	partitions, err := r.partitionCount(ctx)
	if err != nil {
		return failed("RESET", fmt.Errorf("count partitions: %w", err))
	}
	run.row.BrokerPartitions = partitions
	dirs, dirKB, err := deleteDirs(ctx, cycle.Topology.Brokers)
	if err != nil {
		return failed("RESET", fmt.Errorf("count delete directories: %w", err))
	}
	run.row.DeleteDirs, run.row.DeleteDirsKB = dirs, dirKB
	slog.Info("cycle start", "cycle", cycle.ID, "broker_partitions", partitions, "delete_dirs", dirs,
		"delete_dirs_kb", dirKB, "ready_wait_s", waited.Seconds())
	if err := r.reset(ctx, run); err != nil {
		return failed("RESET", err)
	}
	run.state("RESET", began)

	// START
	began = time.Now()
	if err := r.prepare(run, seed); err != nil {
		return failed("START", err)
	}
	if err := r.checkCommitTimes(ctx, run); err != nil {
		return failed("START", err)
	}
	if err := r.startAll(ctx, run); err != nil {
		return failed("START", err)
	}
	stopSampling := r.sampleState(ctx, cycle)
	sampled := false
	finishSampling := func() {
		if sampled {
			return
		}
		sampled = true
		wal, backlog, slots := stopSampling()
		run.timeline.WALRetained, run.timeline.OutboxBacklog, run.timeline.Slots = wal, backlog, slots
		for _, sample := range wal {
			run.row.WALRetainedMax = max(run.row.WALRetainedMax, sample.Value)
		}
		for _, sample := range backlog {
			run.row.OutboxBacklogMax = max(run.row.OutboxBacklogMax, sample.Value)
		}
		for _, slot := range slots {
			run.damage.SlotTimeline = append(run.damage.SlotTimeline, checker.SlotSample{
				Time: slot.At, Slot: slot.Name, Active: slot.Active,
				ConfirmedFlush: slot.ConfirmedFlush, Restart: slot.Restart, CurrentWAL: slot.CurrentWAL,
			})
		}
	}
	defer finishSampling()
	run.state("START", began)

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
	began = time.Now()
	session := load.Start(loadCtx, loadConfig)
	run.session = session
	run.timeline.LoadStart, _ = session.Requests()
	if err := sleep(ctx, max(loadConfig.Warmup-precheckWindow, 0)); err != nil {
		return failed("WARMUP", err)
	}
	if err := r.precheck(ctx, run, session); err != nil {
		return failed("WARMUP", err)
	}
	// The cost interval starts here, at the end of warm-up (SPEC 11.2).
	run.timeline.WarmupEnd = time.Now()
	cpuStart, cpuStartErr := r.docker.CPU(ctx, primary)
	run.timeline.CPUStart = raw.CPUSample{At: time.Now(), TotalNS: cpuStart.container}
	run.state("WARMUP", began)
	if run.row.PreP99MS == 0 {
		// No request succeeded before the fault: a fault on an idle system proves
		// nothing, and "lost 0" here would be false comfort.
		stopLoad()
		report, _ := session.Wait()
		run.row.LoadFailed = report.Failed
		return &failure{"INVALID_NO_LOAD", errors.New("no successful request before the fault")}
	}

	// FAULT and RECOVER
	began = time.Now()
	if len(cycle.Faults) > 0 || cycle.Crash != nil || cycle.Stall != nil {
		if err := r.runFaults(ctx, run, seed); err != nil {
			stopLoad()
			_, _ = session.Wait()
			return err
		}
		run.timeline.FaultEnd, run.row.FaultEndAt = run.faultEnd, run.faultEnd
		run.state("FAULT", began)
		began = time.Now()
		if err := sleep(ctx, time.Duration(cycle.Load.PostFaultS)*time.Second); err != nil {
			return failed("RECOVER", err)
		}
		run.state("RECOVER", began)
	} else {
		if err := sleep(ctx, time.Duration(cycle.Load.DurationS)*time.Second); err != nil {
			return failed("FAULT", err)
		}
		run.state("FAULT", began)
	}

	// DRAIN
	began = time.Now()
	stopLoad()
	report, err := session.Wait()
	run.timeline.LoadStop = time.Now()
	cpuEnd, cpuEndErr := r.docker.CPU(ctx, primary)
	run.timeline.CPUEnd = raw.CPUSample{At: time.Now(), TotalNS: cpuEnd.container}
	// Both samples must come from one primary; a failover kills it.
	run.timeline.CPUValid = cpuStartErr == nil && cpuEndErr == nil && run.row.FailoverAt.IsZero()
	if err != nil {
		return failed("DRAIN", err)
	}
	run.row.ThroughputOps, run.row.LoadS, run.row.LoadFailed = report.ThroughputOps, report.DurationS, report.Failed
	run.row.LatencyP50MS, run.row.LatencyP99MS = report.LatencyP50MS, report.LatencyP99MS
	// A broker restarted by the fault may still be loading its logs; the drain's
	// own clock starts once it serves the cycle topic.
	waited, err = r.waitReady(ctx, cycle.Topology.Brokers, "orders."+cycle.ID, "DRAIN")
	run.row.BrokerWaitS += waited.Seconds()
	if err != nil {
		return failed("DRAIN", err)
	}
	drainStart := time.Now()
	state, drainErr := r.drain(ctx, run, seed)
	run.row.DrainS = time.Since(drainStart).Seconds()
	run.timeline.MarkerIDs, run.timeline.MarkerOffsets = state.markerIDs, state.offsets
	run.timeline.DrainTargetLSN, run.timeline.DrainSlotLSN = state.targetLSN, state.slotLSN
	run.timeline.DrainReason, run.timeline.ConnectStatus = state.reason, state.status
	run.row.DrainMarkerIDs = strings.Join(state.markerIDs, ";")
	run.row.DrainTargetLSN, run.row.DrainSlotLSN, run.row.DrainReason = state.targetLSN, state.slotLSN, state.reason
	observed := time.Now()
	run.timeline.ObservedUntil, run.row.ObservedUntil = observed, observed
	finishSampling()
	if isCDC(cycle.Pattern) {
		// Connect's own log explains CDC damage; keep the part from this cycle.
		if logs, err := r.docker.LogsSince(ctx, connectContainer, run.row.StartedAt); err == nil {
			run.logs.Write(logs)
		}
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
	run.state("DRAIN", began)
	if drainErr != nil {
		return failed("DRAIN", drainErr)
	}

	// DUMP: the raw log first, so CHECK reads the same evidence a recomputation will.
	began = time.Now()
	if err := r.dumpCycle(ctx, run); err != nil {
		return failed("DUMP", err)
	}
	run.state("DUMP", began)

	// CHECK
	began = time.Now()
	checkCtx, cancelCheck := context.WithTimeout(ctx, checkTimeout)
	defer cancelCheck()
	if err := r.writeRaw(checkCtx, run); err != nil {
		return failed("CHECK", err)
	}
	if err := r.crossCheck(checkCtx, run); err != nil {
		return failed("CHECK", err)
	}
	if err := r.check(run); err != nil {
		return failed("CHECK", err)
	}
	run.state("CHECK", began)
	if violation := assertionFailure(cycle, run.row.Result); violation != "" {
		return &failure{"CONTROL_FAIL", errors.New(violation)}
	}
	return nil
}

// checkCommitTimes refuses a cycle whose commit times could be wrong (SPEC 5.1):
// track_commit_timestamp must be on on both servers, and the database's
// transaction ID age well below the freeze limit.
func (r *Runner) checkCommitTimes(ctx context.Context, run *cycleRun) error {
	for _, dsn := range []string{r.measurement.PostgresDSN, replicaDSN} {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("connect to postgres: %w", err)
		}
		var setting string
		err = conn.QueryRow(ctx, `SHOW track_commit_timestamp`).Scan(&setting)
		if err == nil && dsn == r.measurement.PostgresDSN {
			err = conn.QueryRow(ctx, `SELECT age(datfrozenxid) FROM pg_database WHERE datname = current_database()`).Scan(&run.row.XIDAge)
		}
		_ = conn.Close(context.WithoutCancel(ctx))
		if err != nil {
			return fmt.Errorf("read commit-time settings: %w", err)
		}
		if setting != "on" {
			return fmt.Errorf("track_commit_timestamp is %q on %s, want on", setting, dsn)
		}
	}
	if run.row.XIDAge > maxXIDAge {
		return fmt.Errorf("transaction ID age %d is above %d; commit times may be truncated", run.row.XIDAge, maxXIDAge)
	}
	return nil
}

// maxXIDAge is SPEC 5.1's limit, well below autovacuum_freeze_max_age.
const maxXIDAge = 150_000_000

// writeRaw saves everything the checker reads (SPEC 11.5), from the truth
// database, the load generator, the logs and the timeline.
func (r *Runner) writeRaw(ctx context.Context, run *cycleRun) error {
	dir := raw.Dir(r.opts.ResultsDir, run.cycle.ID)
	conn, err := pgx.Connect(ctx, run.truthDSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	truth, err := checker.CollectTruth(ctx, conn, run.row.TruthSource)
	if err != nil {
		return err
	}
	if err := raw.WriteJSON(dir, raw.TruthFile, truth); err != nil {
		return err
	}
	if run.cycle.Pattern == "P3" || run.cycle.Pattern == "P4" {
		outbox, err := checker.CollectOutbox(ctx, conn)
		if err != nil {
			return err
		}
		if err := raw.WriteJSON(dir, raw.OutboxFile, outbox); err != nil {
			return err
		}
	}
	if len(run.cycle.Topology.Consumers) > 0 {
		consumer, err := checker.CollectConsumer(ctx, conn, run.cycle.Topology.Consumers)
		if err != nil {
			return err
		}
		if err := raw.WriteJSON(dir, raw.ConsumerFile, consumer); err != nil {
			return err
		}
	}
	loadData := raw.Load{}
	if run.session != nil {
		loadData.Start, loadData.Requests = run.session.Requests()
	}
	if err := raw.WriteJSON(dir, raw.LoadFile, loadData); err != nil {
		return err
	}
	markers, err := checker.MarkerLines(run.logs.Bytes())
	if err != nil {
		return err
	}
	if err := raw.WriteBytes(dir, raw.MarkersFile, markers); err != nil {
		return err
	}
	if err := raw.WriteJSON(dir, raw.TimelineFile, run.timeline); err != nil {
		return err
	}
	run.rawWritten = true
	return nil
}

// crossCheck reads the topic live with read_committed and compares it with the
// committed view derived from the dump. Any difference fails the cycle: it would
// mean the dump, or its derivation, is not what a consumer saw.
func (r *Runner) crossCheck(ctx context.Context, run *cycleRun) error {
	live, err := checker.ReadKafka(ctx, r.seeds(run.cycle.Topology.Brokers), "orders."+run.cycle.ID)
	if err != nil {
		return err
	}
	records, err := dump.Read(raw.DumpPath(r.opts.ResultsDir, run.cycle.ID))
	if err != nil {
		return err
	}
	derived, err := checker.FromDump(records)
	if err != nil {
		return err
	}
	return sameRecords(live, derived)
}

func sameRecords(live, derived []checker.Record) error {
	order := func(a, b checker.Record) int {
		return cmp.Or(cmp.Compare(a.Partition, b.Partition), cmp.Compare(a.Offset, b.Offset))
	}
	live, derived = slices.Clone(live), slices.Clone(derived)
	slices.SortFunc(live, order)
	slices.SortFunc(derived, order)
	if len(live) != len(derived) {
		return fmt.Errorf("dump gives %d committed records, the live read %d", len(derived), len(live))
	}
	for i := range live {
		if !live[i].Timestamp.Equal(derived[i].Timestamp) || live[i].Pair != derived[i].Pair ||
			live[i].Partition != derived[i].Partition || live[i].Offset != derived[i].Offset || live[i].EventID != derived[i].EventID {
			return fmt.Errorf("record %d differs: dump %+v, live %+v", i, derived[i], live[i])
		}
	}
	return nil
}

// check computes every metric from the raw files alone, exactly as cmd/recompute
// does (SPEC 11.5).
func (r *Runner) check(run *cycleRun) error {
	loaded, err := checker.FromRaw(r.opts.ResultsDir, run.cycle.ID)
	if err != nil {
		return err
	}
	res, rec, cost, damage := Evaluate(loaded)
	run.row.Result, run.row.Recoveries, run.row.Cost = res, rec, cost
	slots := run.damage.SlotTimeline
	run.damage = damage
	run.damage.SlotTimeline = slots
	return nil
}

// Evaluate computes a cycle's metrics from its loaded raw files. The runner and
// cmd/recompute both call it.
func Evaluate(loaded checker.Raw) (checker.Result, checker.Recoveries, checker.Cost, checker.Damage) {
	in, t := loaded.Input, loaded.Timeline
	res, damage := checker.Check(in)
	if !t.FailoverAt.IsZero() {
		damage.LateAfterFailover = checker.LateDeliveries(in, t.FailoverAt)
	}
	timing := checker.Timing{
		LoadStart: t.LoadStart, WarmupEnd: t.WarmupEnd, FirstFault: firstFault(t),
		FaultEnd: t.FaultEnd, LoadStop: t.LoadStop, ObservedUntil: t.ObservedUntil,
	}
	rec := checker.ComputeRecoveries(in, timing, loaded.Load.Requests, res.WindowInvalid)
	cost := checker.ComputeCost(in, t.CPUStart, t.CPUEnd, t.CPUValid, res.WindowInvalid)
	return res, rec, cost, damage
}

// firstFault is the earliest fault injection or point hit.
func firstFault(t raw.Timeline) time.Time {
	first := t.PointHit
	for _, f := range t.Faults {
		if first.IsZero() || (!f.Injected.IsZero() && f.Injected.Before(first)) {
			first = f.Injected
		}
	}
	return first
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
	thermal, warning := thermalState()
	run.row.Thermal = thermal
	var flags []string
	if run.row.PreP99MS > r.measurement.PrecheckMaxP99MS {
		flags = append(flags, "latency")
	}
	if run.row.PreHostLoad > r.measurement.PrecheckMaxHostLoad {
		flags = append(flags, "load")
	}
	if warning {
		flags = append(flags, "thermal")
	}
	run.row.PreFlag = strings.Join(flags, "+")
	return nil
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
func (r *Runner) reset(ctx context.Context, run *cycleRun) error {
	cycle := run.cycle
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
	run.topicCreated = true
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

// keepEvidence stores the damage list and logs of cycles worth explaining.
func (r *Runner) keepEvidence(run *cycleRun) error {
	res := run.row.Result
	damage := run.damage
	business := 0
	for _, b := range res.Business {
		business += b.Total()
	}
	diagnostic := run.cycle.Group == "X" // diagnostic vectors keep all evidence
	if !diagnostic && run.row.Status == "OK" && res.Lost+res.Ghost+res.Duplicates+res.OutOfOrder+business == 0 && !res.WindowInvalid {
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
