package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"dualwrite-study/internal/dump"
	"dualwrite-study/internal/raw"
)

// readyCap bounds one wait for the broker (SPEC 14.2). A broker that restarts
// uncleanly reloads every partition log before it serves requests; 180 s leaves
// room well beyond the reload times seen with a flat partition count.
const readyCap = 180 * time.Second

// waitReady returns once the topology's brokers answer metadata, a controller
// is known and, when topic is set, every partition has a leader and ListOffsets
// succeeds. It logs each retry and returns how long it waited.
func (r *Runner) waitReady(ctx context.Context, brokers int, topic, state string) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, readyCap)
	defer cancel()
	start := time.Now()
	for attempt := 1; ; attempt++ {
		err := r.ready(ctx, brokers, topic)
		if err == nil {
			waited := time.Since(start)
			if attempt > 1 {
				slog.Info("broker ready", "state", state, "topic", topic, "waited_s", waited.Seconds(), "attempts", attempt)
			}
			return waited, nil
		}
		slog.Info("broker not ready, retrying", "state", state, "topic", topic, "attempt", attempt, "error", err)
		if serr := sleep(ctx, time.Second); serr != nil {
			return time.Since(start), fmt.Errorf("broker not ready after %s: %w", time.Since(start).Round(time.Second), err)
		}
	}
}

func (r *Runner) ready(ctx context.Context, brokers int, topic string) error {
	var topics []string
	if topic != "" {
		topics = []string{topic}
	}
	meta, err := r.admin.Metadata(ctx, topics...)
	if err != nil {
		return fmt.Errorf("metadata: %w", err)
	}
	if len(meta.Brokers) < brokers {
		return fmt.Errorf("metadata lists %d of %d brokers", len(meta.Brokers), brokers)
	}
	if meta.Controller < 0 {
		return errors.New("no active controller")
	}
	if topic == "" {
		return nil
	}
	detail, ok := meta.Topics[topic]
	if !ok {
		return fmt.Errorf("topic %s missing from metadata", topic)
	}
	if detail.Err != nil {
		return fmt.Errorf("topic %s: %w", topic, detail.Err)
	}
	for _, p := range detail.Partitions {
		if p.Err != nil || p.Leader < 0 {
			return fmt.Errorf("partition %d has no leader (%v)", p.Partition, p.Err)
		}
	}
	offsets, err := r.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return fmt.Errorf("list end offsets: %w", err)
	}
	return offsets.Error()
}

// partitionCount is the number of partitions the broker holds, internal topics
// included. It is logged at every cycle start to show the count stays flat.
func (r *Runner) partitionCount(ctx context.Context) (int, error) {
	topics, err := r.admin.ListTopicsWithInternal(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, t := range topics {
		n += len(t.Partitions)
	}
	return n, nil
}

// cycleTopics are the topics a cycle creates: its orders topic and, for CDC
// patterns, Debezium's heartbeat topic.
func cycleTopics(cycle Cycle) (orders, heartbeat string) {
	orders = "orders." + cycle.ID
	if isCDC(cycle.Pattern) {
		heartbeat = "__debezium-heartbeat.cdc" + cycleSlug(cycle.ID)
	}
	return orders, heartbeat
}

// dumpCycle writes the cycle topic's raw log and verifies it (SPEC 14.2, DUMP).
// It waits for the broker first, so a broker still reloading is waited for
// rather than failing the dump, and only then reads the brokers' restart lines,
// so a reload that finished during the wait is recorded in this cycle's row.
func (r *Runner) dumpCycle(ctx context.Context, run *cycleRun) error {
	orders, _ := cycleTopics(run.cycle)
	waited, err := r.waitReady(ctx, run.cycle.Topology.Brokers, orders, "DUMP")
	run.row.BrokerWaitS += waited.Seconds()
	r.brokerRestarts(ctx, run)
	if err != nil {
		return err
	}
	stats, err := dump.Topic(ctx, r.seeds(run.cycle.Topology.Brokers), orders, raw.DumpPath(r.opts.ResultsDir, run.cycle.ID))
	if err != nil {
		return fmt.Errorf("dump %s (topic kept): %w", orders, err)
	}
	run.dumped = true
	run.row.DumpRecords, run.row.DumpControl, run.row.DumpBytes, run.row.DumpS = stats.Records, stats.Control, stats.Bytes, stats.Seconds
	slog.Info("dump verified", "cycle", run.cycle.ID, "records", stats.Records, "control", stats.Control,
		"bytes", stats.Bytes, "dump_s", stats.Seconds)
	return nil
}

// deleteCycleTopics removes the cycle's heartbeat topic and, once its dump is
// verified, its orders topic, so the broker's partition count stays flat. A
// topic without a verified dump is kept as the only evidence.
func (r *Runner) deleteCycleTopics(ctx context.Context, run *cycleRun) error {
	orders, heartbeat := cycleTopics(run.cycle)
	var errs []error
	if heartbeat != "" {
		if err := r.deleteTopic(ctx, heartbeat); err != nil {
			errs = append(errs, err)
		}
	}
	if run.dumped {
		if err := r.deleteTopic(ctx, orders); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Runner) deleteTopic(ctx context.Context, topic string) error {
	resp, err := r.admin.DeleteTopic(ctx, topic)
	if err == nil {
		err = resp.Err
	}
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete topic %s: %w", topic, err)
	}
	// Deletion reaches the broker's metadata a moment later. Wait for it, so the
	// next cycle's partition count does not include this topic.
	ctx, cancel := context.WithTimeout(ctx, deleteWait)
	defer cancel()
	for {
		meta, err := r.admin.Metadata(ctx, topic)
		if err == nil {
			if t, ok := meta.Topics[topic]; !ok || errors.Is(t.Err, kerr.UnknownTopicOrPartition) {
				return nil
			}
		}
		if err := sleep(ctx, 100*time.Millisecond); err != nil {
			return fmt.Errorf("topic %s still listed %s after delete: %w", topic, deleteWait, err)
		}
	}
}

// deleteWait bounds the wait for a deleted topic to leave the metadata.
const deleteWait = 30 * time.Second

// deleteDirsScript lists each partition directory marked for deletion with its
// size in KB. Kafka removes such directories on its own schedule, so one can
// vanish between the listing and du; du's error is discarded, the directory
// drops out of the count, and the script always exits 0.
const deleteDirsScript = `for d in /var/lib/kafka/data/*-delete; do [ -d "$d" ] && du -sk "$d" 2>/dev/null; done; exit 0`

// deleteDirs counts the partition directories a broker has marked for deletion
// but not yet removed (file.delete.delay.ms), and their size, across the
// topology's brokers. They are reloaded at every broker restart.
func deleteDirs(ctx context.Context, brokers int) (int, int64, error) {
	count, kb := 0, int64(0)
	for i := 1; i <= max(brokers, 1); i++ {
		out, err := exec.CommandContext(ctx, "docker", "exec", fmt.Sprintf("dualwrite-broker-%d-1", i), "sh", "-c", deleteDirsScript).Output()
		if err != nil {
			return 0, 0, fmt.Errorf("broker %d: %w", i, err)
		}
		n, size, err := parseDeleteDirs(string(out))
		if err != nil {
			return 0, 0, fmt.Errorf("broker %d: %w", i, err)
		}
		count, kb = count+n, kb+size
	}
	return count, kb, nil
}

// parseDeleteDirs reads du's "<kb>\t<path>" lines.
func parseDeleteDirs(out string) (int, int64, error) {
	n, kb := 0, int64(0)
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		size, _, _ := strings.Cut(line, "\t")
		v, err := strconv.ParseInt(strings.TrimSpace(size), 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("parse %q: %w", line, err)
		}
		n, kb = n+1, kb+v
	}
	return n, kb, nil
}

// reloadLine is the broker's log line after it has loaded its partition logs.
// "unclean log dirs" means the previous shutdown did not complete.
var reloadLine = regexp.MustCompile(`Loaded (\d+) logs in (\d+)ms( \(unclean log dirs)?`)

// brokerRestarts reads the brokers' logs since the cycle started and records
// whether any broker restarted during it, cleanly or not, and its reload time.
// Called after a wait for the broker, so a reload in progress has finished.
func (r *Runner) brokerRestarts(ctx context.Context, run *cycleRun) {
	var kinds []string
	for i := 1; i <= max(run.cycle.Topology.Brokers, 1); i++ {
		logs, err := r.docker.LogsSince(ctx, fmt.Sprintf("dualwrite-broker-%d-1", i), run.row.StartedAt)
		if err != nil {
			slog.Warn("read broker logs", "broker", i, "error", err)
			continue
		}
		for _, m := range reloadLine.FindAllSubmatch(logs, -1) {
			kind := "clean"
			if len(m[3]) > 0 {
				kind = "unclean"
			}
			ms, _ := strconv.ParseInt(string(m[2]), 10, 64)
			kinds = append(kinds, kind)
			run.row.BrokerReloadMS = max(run.row.BrokerReloadMS, ms)
			slog.Info("broker restarted", "cycle", run.cycle.ID, "broker", i, "shutdown", kind,
				"logs", string(m[1]), "reload_ms", ms)
		}
	}
	run.row.BrokerShutdown = strings.Join(kinds, ";")
}
