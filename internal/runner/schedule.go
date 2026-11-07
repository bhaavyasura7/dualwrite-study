package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"dualwrite-study/internal/faults"
	"dualwrite-study/internal/obs"
)

const (
	replicaDSN      = "postgres://app:app@localhost:55433/shop"
	rebuildTimeout  = 180 * time.Second
	failoverNetwork = "dualwrite_failover"
)

// uniform draws from the range using the cycle's random source.
func uniform(rng *rand.Rand, r Range) time.Duration {
	return time.Duration((r.Lo + rng.Float64()*(r.Hi-r.Lo)) * float64(time.Second))
}

// runFaults arms any crash or stall point, runs the fault schedule and restores
// the system. Faults are always recovered, even after a failure, because a paused
// or stopped container would poison the next cycle.
func (r *Runner) runFaults(ctx context.Context, run *cycleRun, seed uint64) error {
	cycle := run.cycle
	rng := rand.New(rand.NewPCG(seed, seed+2))
	began := time.Now()
	crashed := make(map[string]bool) // Go components to recreate once the faults end
	var mu sync.Mutex
	markCrashed := func(names ...string) {
		mu.Lock()
		defer mu.Unlock()
		for _, name := range names {
			crashed[name] = true
		}
	}

	var labels []string
	var wg sync.WaitGroup
	errs := make(chan error, len(cycle.Faults)+1)
	latest := time.Duration(0)
	for i := range cycle.Faults {
		spec := cycle.Faults[i]
		fault, err := r.buildFault(run, spec, rng, markCrashed)
		if err != nil {
			return failed("FAULT", err)
		}
		labels = append(labels, spec.Type)
		var at, duration time.Duration
		if spec.AtS != nil {
			at = uniform(rng, *spec.AtS)
		}
		if spec.DurationS != nil {
			duration = uniform(rng, *spec.DurationS)
		}
		latest = max(latest, at+duration)
		wg.Go(func() {
			if err := r.playFault(ctx, run, spec, fault, began.Add(at), duration); err != nil {
				errs <- err
			}
		})
	}
	if run.row.Fault == "" {
		run.row.Fault = strings.Join(labels, "+")
	}

	var pointErr error
	if len(run.pointEnv) > 0 {
		pointErr = r.awaitPoint(ctx, run, latest, markCrashed)
	}
	wg.Wait()
	close(errs)

	recoverCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recoverTimeout)
	defer cancel()
	var recoverErr error
	for _, component := range run.components {
		if crashed[component.Name] {
			if err := r.captureLogs(recoverCtx, run, component.Name); err != nil {
				recoverErr = errors.Join(recoverErr, err)
			}
			if err := r.startComponent(recoverCtx, component, run.env); err != nil {
				recoverErr = errors.Join(recoverErr, err)
			}
		}
	}
	run.faultEnd = time.Now()
	if pointErr != nil {
		return pointErr
	}
	for err := range errs {
		return failed("FAULT", err)
	}
	if recoverErr != nil {
		return failed("RECOVER", recoverErr)
	}
	return nil
}

// playFault waits for the fault's moment, then injects and recovers it.
func (r *Runner) playFault(ctx context.Context, run *cycleRun, spec Fault, fault faults.Fault, at time.Time, duration time.Duration) error {
	if spec.OnLog != "" {
		wait, cancel := context.WithTimeout(ctx, faultTimeout)
		defer cancel()
		if err := r.docker.WaitForLog(wait, pointTarget(run.cycle), spec.OnLog); err != nil {
			return fmt.Errorf("%s never saw %s: %w", fault.Name(), spec.OnLog, err)
		}
	} else if err := sleep(ctx, time.Until(at)); err != nil {
		return err
	}
	slog.Info("fault injected", "cycle", run.cycle.ID, "fault", fault.Name(), "target", spec.Target)
	injectErr := fault.Inject(ctx)
	if injectErr == nil {
		_ = sleep(ctx, duration)
	}
	restore, cancel := context.WithTimeout(context.WithoutCancel(ctx), recoverTimeout)
	defer cancel()
	return errors.Join(injectErr, fault.Recover(restore))
}

// awaitPoint arms the point now that warm-up is over and waits for it to fire.
func (r *Runner) awaitPoint(ctx context.Context, run *cycleRun, latest time.Duration, markCrashed func(...string)) error {
	target := pointTarget(run.cycle)
	wait, cancel := context.WithTimeout(ctx, latest+faultTimeout)
	defer cancel()
	if err := r.docker.Signal(wait, target, "SIGUSR1"); err != nil {
		return failed("FAULT", err)
	}
	if run.cycle.Stall != nil {
		if err := r.docker.WaitForLog(wait, target, obs.StallPointEnd); err != nil {
			return &failure{"INVALID_NO_STALL", err}
		}
		return nil
	}
	if err := r.docker.WaitForLog(wait, target, obs.CrashPointHit); err != nil {
		_ = r.captureLogs(context.WithoutCancel(ctx), run, target)
		return &failure{"INVALID_NO_CRASH", err}
	}
	code, err := r.docker.WaitForExit(wait, target)
	if err != nil {
		return failed("RECOVER", err)
	}
	if code != 137 {
		return failed("RECOVER", fmt.Errorf("exit code %d, want 137 (SIGKILL)", code))
	}
	markCrashed(target)
	return nil
}

// buildFault maps a catalogue fault onto its containers or proxies.
func (r *Runner) buildFault(run *cycleRun, spec Fault, rng *rand.Rand, markCrashed func(...string)) (faults.Fault, error) {
	toxics := r.toxics
	if strings.HasPrefix(spec.Target, "pg-") {
		toxics = r.pgToxics
	}
	switch spec.Type {
	case "crash":
		names, err := containers(run, spec.Target, rng)
		if err != nil {
			return nil, err
		}
		var goComponents []string
		for _, name := range names {
			if name != connectContainer && name != broker {
				goComponents = append(goComponents, name)
			}
		}
		restore := func(ctx context.Context) error {
			if len(goComponents) > 0 {
				markCrashed(goComponents...) // recreated after the last fault ends
				return nil
			}
			var errs []error
			for _, name := range names {
				errs = append(errs, r.docker.Start(ctx, name))
			}
			return errors.Join(errs...)
		}
		return faults.Crash(r.docker, names, restore), nil
	case "graceful_stop":
		names, err := containers(run, spec.Target, rng)
		return wrap(err, func() faults.Fault { return faults.GracefulStop(r.docker, names) })
	case "restart":
		names, err := containers(run, spec.Target, rng)
		return wrap(err, func() faults.Fault { return faults.Restart(r.docker, names) })
	case "pause":
		names, err := containers(run, spec.Target, rng)
		return wrap(err, func() faults.Fault { return faults.Pause(r.docker, names) })
	case "broker_stop":
		names, err := containers(run, spec.Target, rng)
		return wrap(err, func() faults.Fault { return faults.BrokerStop(r.docker, names) })
	case "proxy_disable":
		names, err := proxies(spec.Target, run.cycle.Topology.Brokers)
		return wrap(err, func() faults.Fault { return faults.ProxyDisable(toxics, names) })
	case "latency":
		names, err := proxies(spec.Target, run.cycle.Topology.Brokers)
		latency, _ := spec.Params["latency_ms"].(int)
		jitter, _ := spec.Params["jitter_ms"].(int)
		return wrap(err, func() faults.Fault { return faults.Latency(toxics, names, latency, jitter) })
	case "withhold_downstream":
		names, err := proxies(spec.Target, run.cycle.Topology.Brokers)
		return wrap(err, func() faults.Fault { return faults.WithholdDownstream(toxics, names) })
	case "reset_peer":
		names, err := proxies(spec.Target, run.cycle.Topology.Brokers)
		toxicity, ok := toFloat(spec.Params["toxicity"])
		if !ok {
			return nil, fmt.Errorf("reset_peer needs a toxicity, got %v", spec.Params["toxicity"])
		}
		return wrap(err, func() faults.Fault { return faults.ResetPeer(toxics, names, float32(toxicity)) })
	case "kill_leader":
		var leader string
		inject := func(ctx context.Context) error {
			var err error
			if leader, err = r.partitionLeader(ctx, "orders."+run.cycle.ID); err != nil {
				return err
			}
			slog.Info("killing partition leader", "cycle", run.cycle.ID, "broker", leader)
			return r.docker.Kill(ctx, leader)
		}
		return faults.New("kill_leader", inject, func(ctx context.Context) error { return r.docker.Start(ctx, leader) }), nil
	case "failover":
		return faults.New("failover", func(ctx context.Context) error { return r.failover(ctx, run) },
			func(context.Context) error { return nil }), nil
	}
	return nil, fmt.Errorf("fault type %s is not built yet", spec.Type)
}

func wrap(err error, build func() faults.Fault) (faults.Fault, error) {
	if err != nil {
		return nil, err
	}
	return build(), nil
}

// containers resolves a catalogue target to container names.
func containers(run *cycleRun, target string, rng *rand.Rand) ([]string, error) {
	var relays []string
	for _, component := range run.components {
		if strings.HasPrefix(component.Name, relayName) {
			relays = append(relays, component.Name)
		}
	}
	switch target {
	case "orderservice":
		return []string{orderService}, nil
	case "relay":
		return relays, nil
	case "relay-1":
		return []string{relayName + "1"}, nil
	case "relay-any":
		if len(relays) == 0 {
			return nil, errors.New("no relays in this cycle")
		}
		return []string{relays[rng.IntN(len(relays))]}, nil
	case "connect":
		return []string{connectContainer}, nil
	case "kafka":
		return []string{broker}, nil
	}
	return nil, fmt.Errorf("unknown container target %s", target)
}

// proxies resolves a catalogue target to Toxiproxy proxy names.
func proxies(target string, brokers int) ([]string, error) {
	switch target {
	case "kafka-app", "kafka-connect":
		names := make([]string, brokers)
		for i := range names {
			names[i] = fmt.Sprintf("%s-b%d", target, i+1)
		}
		return names, nil
	case "pg-app", "pg-connect":
		return []string{target}, nil
	case "pg-all":
		return []string{"pg-app", "pg-connect"}, nil
	}
	return nil, fmt.Errorf("unknown proxy target %s", target)
}

// partitionLeader names the container leading partition 0 of the topic.
func (r *Runner) partitionLeader(ctx context.Context, topic string) (string, error) {
	topics, err := r.admin.ListTopics(ctx, topic)
	if err != nil {
		return "", fmt.Errorf("list %s: %w", topic, err)
	}
	partition, ok := topics[topic].Partitions[0]
	if !ok {
		return "", fmt.Errorf("%s has no partition 0", topic)
	}
	return fmt.Sprintf("dualwrite-broker-%d-1", partition.Leader), nil
}

// composeEnv is the environment Compose needs for the broker count.
func composeEnv(brokers int) []string {
	env := os.Environ()
	if brokers == 3 {
		env = append(env,
			"KAFKA_VOTERS=1@broker-1:9091,2@broker-2:9091,3@broker-3:9091",
			"KAFKA_INTERNAL_RF=3",
			"CONNECT_BOOTSTRAP=toxiproxy:19191,toxiproxy:19192,toxiproxy:19193",
			"CONNECT_RF=3",
			"CONNECT_TOPIC_SUFFIX="+threeBrokerSuffix,
			"KAFKA_VOLUME_1=kafka-b3-1",
		)
	}
	return env
}

// threeBrokerSuffix keeps three-broker Connect topics apart from one-broker ones.
const threeBrokerSuffix = "-b3"

// connectTopics names Connect's internal topics for the broker count.
func connectTopics(brokers int) []string {
	suffix := ""
	if brokers == 3 {
		suffix = threeBrokerSuffix
	}
	return []string{"connect-configs" + suffix, "connect-offsets" + suffix, "connect-status" + suffix}
}

// currentBrokers reads the running topology from the broker's quorum setting.
func (r *Runner) currentBrokers(ctx context.Context) (int, error) {
	voters, err := r.docker.EnvValue(ctx, broker, "KAFKA_CONTROLLER_QUORUM_VOTERS")
	if err != nil {
		return 0, err
	}
	if strings.Contains(voters, "broker-3") {
		return 3, nil
	}
	return 1, nil
}

// ensureBrokers switches between one and three brokers. Each topology has its own
// volumes and Connect topics, so nothing leaks across. Connect starts only after
// every broker is healthy. Cycles are grouped so this is rare.
func (r *Runner) ensureBrokers(ctx context.Context, brokers int) error {
	current, err := r.currentBrokers(ctx)
	if err != nil {
		return err
	}
	if current == brokers {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, rebuildTimeout)
	defer cancel()
	slog.Info("switching broker count", "from", current, "to", brokers)
	services := []string{"broker-1"}
	if brokers == 3 {
		services = []string{"broker-1", "broker-2", "broker-3"}
	}
	// The three-broker volumes are one unit: a quorum cannot mix old and new
	// storage. They hold nothing worth keeping, as each cycle has its own topic.
	steps := [][]string{
		{"compose", "--profile", "brokers3", "rm", "-sfv", "kafka-connect", "broker-1", "broker-2", "broker-3"},
		{"volume", "rm", "-f", "dualwrite_kafka-b3-1", "dualwrite_kafka-b3-2", "dualwrite_kafka-b3-3"},
		append([]string{"compose", "--profile", "brokers3", "up", "-d", "--wait", "--no-deps"}, services...),
		{"compose", "up", "-d", "--wait", "--no-deps", "kafka-connect"},
	}
	for _, args := range steps {
		command := exec.CommandContext(ctx, "docker", args...)
		command.Env = composeEnv(brokers)
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, output)
		}
	}
	return r.waitForConnect(ctx)
}

// waitForConnect waits until Connect has created its internal topics.
func (r *Runner) waitForConnect(ctx context.Context) error {
	for {
		if _, err := r.connectRequest(ctx, http.MethodGet, "/connectors", nil); err == nil {
			if err := r.Preflight(ctx); err == nil {
				return nil
			}
		}
		if err := sleep(ctx, time.Second); err != nil {
			return fmt.Errorf("connect never came back: %w", err)
		}
	}
}

// failover kills the primary, promotes the replica, gives it the primary's name
// and applies CDC_FAILOVER_RECOVERY (SPEC 9.3, 12). The primary must die first:
// cutting traffic beforehand would let the replica catch up and hide ghost events.
// From here on the promoted replica is the truth.
func (r *Runner) failover(ctx context.Context, run *cycleRun) error {
	r.recordLag(ctx, run)
	run.row.FailoverAt = time.Now()
	if err := r.docker.Kill(ctx, primary); err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, replicaDSN)
	if err != nil {
		return fmt.Errorf("connect to replica: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	if _, err := conn.Exec(ctx, `SELECT pg_promote(true, 30)`); err != nil {
		return fmt.Errorf("promote replica: %w", err)
	}
	run.truthDSN = replicaDSN
	if err := r.docker.ConnectAlias(ctx, failoverNetwork, replica, "postgres-primary"); err != nil {
		return err
	}
	if err := r.restartPGProxy(ctx); err != nil {
		return err
	}
	if !isCDC(run.cycle.Pattern) {
		return nil
	}
	if err := r.deleteConnectors(ctx); err != nil {
		return err
	}
	snapshot := "no_data"
	if run.cycle.Env["CDC_FAILOVER_RECOVERY"] == "new_slot_initial_snapshot" {
		snapshot = "initial"
	}
	return r.createConnector(ctx, run.cycle, connector{suffix: "_f", snapshot: snapshot}, replicaDSN)
}

// recordLag reads replication lag just before the primary dies, so ghost events
// can be read against how far behind the replica was.
func (r *Runner) recordLag(ctx context.Context, run *cycleRun) {
	conn, err := pgx.Connect(ctx, r.measurement.PostgresDSN)
	if err != nil {
		slog.Warn("could not read replication lag", "error", err)
		return
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var bytes int64
	var ms float64
	err = conn.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint,
		COALESCE(EXTRACT(EPOCH FROM replay_lag) * 1000, 0)::float8
		FROM pg_stat_replication WHERE application_name = 'walreceiver'`).Scan(&bytes, &ms)
	if err != nil {
		slog.Warn("could not read replication lag", "error", err)
		return
	}
	run.row.FailoverLagBytes = strconv.FormatInt(bytes, 10)
	run.row.FailoverLagMS = strconv.FormatFloat(ms, 'f', 3, 64)
}

// applyReplicationLatency delays the stream from primary to replica through the
// pg-repl proxy. RESET has already removed any earlier toxic.
func (r *Runner) applyReplicationLatency(c Cycle) error {
	latency, err := strconv.Atoi(c.Env["REPL_LATENCY_MS"])
	if err != nil {
		return fmt.Errorf("REPL_LATENCY_MS: %w", err)
	}
	if latency == 0 {
		return nil
	}
	return faults.Latency(r.pgToxics, []string{"pg-repl"}, latency, 0).Inject(context.Background())
}

// rebuild restores the original primary and replica after a failover cycle.
func (r *Runner) rebuild(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, rebuildTimeout)
	defer cancel()
	if err := r.deleteConnectors(ctx); err != nil {
		return err
	}
	for _, args := range [][]string{
		{"compose", "rm", "-sfv", "postgres-primary", "postgres-replica"},
		{"volume", "rm", "dualwrite_pg-primary", "dualwrite_pg-replica"},
		{"compose", "up", "-d", "--wait", "postgres-primary", "postgres-replica"},
	} {
		if output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
			return fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, output)
		}
	}
	return r.restartPGProxy(ctx)
}

// restartPGProxy clears any dial stuck on a dead address. Toxiproxy dials upstream
// inside its accept loop, so such a dial would block every new Postgres client and
// inflate downtime. Only connections to the dead primary are affected.
func (r *Runner) restartPGProxy(ctx context.Context) error {
	if err := r.docker.Kill(ctx, pgProxy); err != nil {
		return err
	}
	if _, err := r.docker.WaitForExit(ctx, pgProxy); err != nil {
		return err
	}
	return r.docker.Start(ctx, pgProxy)
}

// setReplication switches between asynchronous and synchronous commit (D-4).
func setReplication(ctx context.Context, conn *pgx.Conn, mode string) error {
	standby := ""
	if mode == "sync" {
		standby = "*"
	}
	if _, err := conn.Exec(ctx, "ALTER SYSTEM SET synchronous_standby_names = "+"'"+standby+"'"); err != nil {
		return fmt.Errorf("set replication mode: %w", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_reload_conf()`); err != nil {
		return fmt.Errorf("reload config: %w", err)
	}
	return nil
}
