package runner

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"dualwrite-study/internal/checker"
)

// strategies are the consumer strategies, in column order.
var strategies = []string{"naive", "dedup", "upsert"}

// businessClasses are the per-order damage classes, in column order (SPEC 11.2).
var businessClasses = []string{
	"missing_payments", "duplicate_payments", "unexpected_payments",
	"incorrect_payment_amounts", "incorrect_order_mappings",
}

func businessColumns() []string {
	var out []string
	for _, s := range strategies {
		for _, c := range businessClasses {
			out = append(out, c+"_"+s)
		}
	}
	return out
}

// columns is the results CSV header (SPEC 14.3).
var columns = slices.Concat([]string{
	"run_id", "cycle_id", "vector_id", "pattern", "mode", "fault", "crash_point", "repeat", "seed",
	"status", "sweep_json", "env_json", "versions_json", "load_rate", "orders_total", "events_total",
	"lost", "ghost", "duplicates", "out_of_order", "stale_redelivery", "lost_after_send", "lost_after_ack",
	"hooked_lost", "hooked_dup", "collateral_lost", "collateral_dup",
	"business_damage_naive", "business_damage_dedup", "business_damage_upsert",
}, businessColumns(), []string{
	"wal_retained_bytes_max", "outbox_backlog_max", "relay_batches", "batch_rows_mean", "batch_rows_max",
	"send_failed", "crash_after_n", "inflight_at_crash", "oldest_pending_ms",
	"failover_at", "failover_lag_bytes", "failover_lag_ms", "truth_source", "host", "shard", "git_commit",
	"window_p50_ms", "window_p99_ms", "window_max_ms", "window_p99_pre_ms", "window_valid",
	"commit_ts_missing", "window_negative", "xid_age",
	"fault_end_at", "observed_until",
	"service_recovery_s", "service_recovered", "synchronization_recovery_s", "synchronization_recovered",
	"consistency_recovery_s", "consistency_recovered",
	"throughput_ops", "latency_p50_ms", "latency_p99_ms", "load_s", "load_failed",
	"db_cpu_pct", "db_cpu_s", "cpu_window_s", "committed_changes", "changes_per_s", "orders_per_s", "db_cpu_s_per_1000_changes",
	"pre_host_load", "pre_db_cpu_pct", "pre_p99_ms", "pre_flag", "thermal", "run_order", "started_at", "finished_at",
	"broker_partitions", "delete_dirs", "delete_dirs_kb", "broker_wait_s", "broker_shutdown", "broker_reload_ms",
	"drain_s", "drain_reason", "drain_marker_ids", "drain_target_lsn", "drain_slot_lsn",
	"dump_records", "dump_control_records", "dump_bytes", "dump_s",
})

// Row is one cycle's result.
type Row struct {
	RunID, CycleID, VectorID, Pattern, Mode, Fault, CrashPoint string
	Repeat                                                     int
	Seed                                                       uint64
	Status, SweepJSON, EnvJSON, VersionsJSON                   string
	LoadFailed                                                 int
	Thermal                                                    string
	Order                                                      int // position in the run's randomised order
	FailoverLagBytes, FailoverLagMS                            string
	FailoverAt                                                 time.Time
	TruthSource                                                string
	XIDAge                                                     int64
	LoadRate                                                   int
	Consumers                                                  bool // the cycle ran group G consumers
	Result                                                     checker.Result
	Recoveries                                                 checker.Recoveries
	Cost                                                       checker.Cost
	FaultEndAt, ObservedUntil                                  time.Time
	CrashAfterN                                                int
	Host, Shard, GitCommit                                     string
	ThroughputOps, LatencyP50MS, LatencyP99MS                  float64
	LoadS                                                      float64
	PreHostLoad, PreDBCPUPct, PreP99MS                         float64
	PreFlag                                                    string
	WALRetainedMax                                             int64
	OutboxBacklogMax                                           int64
	StartedAt, FinishedAt                                      time.Time
	BrokerPartitions                                           int // partitions on the broker at cycle start
	DeleteDirs                                                 int // partition directories marked for deletion, at cycle start
	DeleteDirsKB                                               int64
	BrokerWaitS                                                float64 // time spent waiting for the broker to be ready
	BrokerShutdown                                             string  // clean or unclean, per broker restart in the cycle
	BrokerReloadMS                                             int64   // longest partition-log reload after a restart
	DrainS                                                     float64
	DrainReason, DrainMarkerIDs, DrainTargetLSN, DrainSlotLSN  string
	DumpRecords, DumpControl, DumpBytes                        int64
	DumpS                                                      float64
}

func number(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }

func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// MetricColumns are the columns the checker computes from a cycle's raw files
// alone (SPEC 11.5). cmd/recompute rewrites exactly these.
var MetricColumns = slices.Concat([]string{
	"orders_total", "events_total",
	"lost", "ghost", "duplicates", "out_of_order", "stale_redelivery", "lost_after_send", "lost_after_ack",
	"hooked_lost", "hooked_dup", "collateral_lost", "collateral_dup",
	"business_damage_naive", "business_damage_dedup", "business_damage_upsert",
}, businessColumns(), []string{
	"relay_batches", "batch_rows_mean", "batch_rows_max", "send_failed", "inflight_at_crash", "oldest_pending_ms",
	"window_p50_ms", "window_p99_ms", "window_max_ms", "window_p99_pre_ms", "window_valid",
	"commit_ts_missing", "window_negative",
	"service_recovery_s", "service_recovered", "synchronization_recovery_s", "synchronization_recovered",
	"consistency_recovery_s", "consistency_recovered",
	"db_cpu_pct", "db_cpu_s", "cpu_window_s", "committed_changes", "changes_per_s", "orders_per_s", "db_cpu_s_per_1000_changes",
})

// MetricFields formats the metric columns. The runner and cmd/recompute both
// use it, so a recomputation can be compared with results.csv text for text.
func MetricFields(res checker.Result, rec checker.Recoveries, cost checker.Cost, pattern string, consumers bool) map[string]string {
	count := strconv.Itoa
	f := map[string]string{
		"orders_total": count(res.OrdersTotal), "events_total": count(res.EventsTotal),
		"lost": count(res.Lost), "ghost": count(res.Ghost), "duplicates": count(res.Duplicates),
		"out_of_order": count(res.OutOfOrder), "stale_redelivery": count(res.StaleRedelivery),
		"lost_after_send": count(res.LostAfterSend), "lost_after_ack": "",
		"hooked_lost": count(res.HookedLost), "hooked_dup": count(res.HookedDup),
		"collateral_lost": count(res.CollateralLost), "collateral_dup": count(res.CollateralDup),
		"relay_batches": count(res.RelayBatches), "batch_rows_mean": number(res.BatchRowsMean), "batch_rows_max": count(res.BatchRowsMax),
		"send_failed": count(res.SendFailed), "inflight_at_crash": count(res.InflightAtCrash), "oldest_pending_ms": number(res.OldestPendingMS),
		"commit_ts_missing": count(res.CommitTSMissing), "window_negative": count(res.WindowNegative),
		"window_valid": strconv.FormatBool(!res.WindowInvalid),
	}
	// P4 and P5 publish through Connect, which logs no delivery markers.
	if !isCDC(pattern) {
		f["lost_after_ack"] = count(res.LostAfterAck)
	}
	for _, column := range []string{"window_p50_ms", "window_p99_ms", "window_max_ms", "window_p99_pre_ms"} {
		f[column] = ""
	}
	if !res.WindowInvalid {
		f["window_p50_ms"], f["window_p99_ms"], f["window_max_ms"] = number(res.WindowP50MS), number(res.WindowP99MS), number(res.WindowMaxMS)
	}
	if rec.WindowP99PreKnown {
		f["window_p99_pre_ms"] = number(rec.WindowP99PreMS)
	}
	for _, s := range strategies {
		b, ok := res.Business[s]
		values := []int{b.Missing, b.Duplicate, b.Unexpected, b.WrongAmount, b.WrongMapping}
		f["business_damage_"+s] = ""
		for i, class := range businessClasses {
			f[class+"_"+s] = ""
			if ok && consumers {
				f[class+"_"+s] = count(values[i])
			}
		}
		if ok && consumers {
			f["business_damage_"+s] = count(b.Total())
		}
	}
	for name, r := range map[string]checker.Recovery{
		"service": rec.Service, "synchronization": rec.Synchronization, "consistency": rec.Consistency,
	} {
		f[name+"_recovery_s"], f[name+"_recovered"] = "", ""
		if r.Known {
			f[name+"_recovery_s"], f[name+"_recovered"] = number(r.Seconds), strconv.FormatBool(r.Recovered)
		}
	}
	for _, column := range []string{"db_cpu_pct", "db_cpu_s", "cpu_window_s", "committed_changes", "changes_per_s", "orders_per_s", "db_cpu_s_per_1000_changes"} {
		f[column] = ""
	}
	if cost.Known {
		f["db_cpu_pct"], f["db_cpu_s"], f["cpu_window_s"] = number(cost.DBCPUPct), number(cost.DBCPUS), number(cost.WindowS)
		f["committed_changes"], f["changes_per_s"], f["orders_per_s"] = count(cost.CommittedChanges), number(cost.ChangesPerS), number(cost.OrdersPerS)
		if cost.CommittedChanges > 0 {
			f["db_cpu_s_per_1000_changes"] = number(cost.PerThousand)
		}
	}
	return f
}

func (r Row) record() []string {
	count := strconv.Itoa
	wal := ""
	if r.WALRetainedMax > 0 {
		wal = strconv.FormatInt(r.WALRetainedMax, 10)
	}
	backlog := ""
	if r.Pattern == "P3" {
		backlog = strconv.FormatInt(r.OutboxBacklogMax, 10)
	}
	xidAge := ""
	if r.XIDAge > 0 {
		xidAge = strconv.FormatInt(r.XIDAge, 10)
	}
	f := MetricFields(r.Result, r.Recoveries, r.Cost, r.Pattern, r.Consumers)
	for k, v := range map[string]string{
		"run_id": r.RunID, "cycle_id": r.CycleID, "vector_id": r.VectorID, "pattern": r.Pattern, "mode": r.Mode,
		"fault": r.Fault, "crash_point": r.CrashPoint, "repeat": count(r.Repeat), "seed": strconv.FormatUint(r.Seed, 10),
		"status": r.Status, "sweep_json": r.SweepJSON, "env_json": r.EnvJSON, "versions_json": r.VersionsJSON,
		"load_rate":              count(r.LoadRate),
		"wal_retained_bytes_max": wal, "outbox_backlog_max": backlog, "crash_after_n": count(r.CrashAfterN),
		"failover_at": stamp(r.FailoverAt), "failover_lag_bytes": r.FailoverLagBytes, "failover_lag_ms": r.FailoverLagMS,
		"truth_source": r.TruthSource, "host": r.Host, "shard": r.Shard, "git_commit": r.GitCommit,
		"xid_age": xidAge, "fault_end_at": stamp(r.FaultEndAt), "observed_until": stamp(r.ObservedUntil),
		"throughput_ops": number(r.ThroughputOps), "latency_p50_ms": number(r.LatencyP50MS), "latency_p99_ms": number(r.LatencyP99MS),
		"load_s": number(r.LoadS), "load_failed": count(r.LoadFailed),
		"pre_host_load": number(r.PreHostLoad), "pre_db_cpu_pct": number(r.PreDBCPUPct), "pre_p99_ms": number(r.PreP99MS),
		"pre_flag": r.PreFlag, "thermal": r.Thermal, "run_order": count(r.Order),
		"started_at": r.StartedAt.UTC().Format(time.RFC3339), "finished_at": r.FinishedAt.UTC().Format(time.RFC3339),
		"broker_partitions": count(r.BrokerPartitions), "delete_dirs": count(r.DeleteDirs),
		"delete_dirs_kb": strconv.FormatInt(r.DeleteDirsKB, 10), "broker_wait_s": number(r.BrokerWaitS),
		"broker_shutdown": r.BrokerShutdown, "broker_reload_ms": strconv.FormatInt(r.BrokerReloadMS, 10),
		"drain_s": number(r.DrainS), "drain_reason": r.DrainReason, "drain_marker_ids": r.DrainMarkerIDs,
		"drain_target_lsn": r.DrainTargetLSN, "drain_slot_lsn": r.DrainSlotLSN,
		"dump_records": strconv.FormatInt(r.DumpRecords, 10), "dump_control_records": strconv.FormatInt(r.DumpControl, 10),
		"dump_bytes": strconv.FormatInt(r.DumpBytes, 10), "dump_s": number(r.DumpS),
	} {
		f[k] = v
	}
	out := make([]string, len(columns))
	for i, column := range columns {
		value, ok := f[column]
		if !ok {
			panic("results: no value for column " + column)
		}
		out[i] = value
	}
	return out
}

// Results appends rows to a CSV file and remembers which cycles it holds.
type Results struct {
	path string
	done map[string]struct{}
}

// OpenResults reads an existing file so a run can resume. Rows from another
// commit are refused: a resumed run must stay within one build.
func OpenResults(path, commit string) (*Results, error) {
	results := &Results{path: path, done: make(map[string]struct{})}
	rows, err := readRows(path)
	if errors.Is(err, os.ErrNotExist) {
		return results, nil
	}
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if row["git_commit"] != commit {
			return nil, fmt.Errorf("%s holds rows from commit %s, this run is %s", path, row["git_commit"], commit)
		}
		results.done[row["cycle_id"]] = struct{}{}
	}
	return results, nil
}

// Done reports whether the cycle already has a row.
func (r *Results) Done(cycleID string) bool {
	_, ok := r.done[cycleID]
	return ok
}

// Append writes one row and syncs it, so a crash of the runner loses nothing.
func (r *Results) Append(row Row) error {
	if err := os.MkdirAll(filepath.Dir(r.path), 0o755); err != nil {
		return fmt.Errorf("create results directory: %w", err)
	}
	_, statErr := os.Stat(r.path)
	file, err := os.OpenFile(r.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open results: %w", err)
	}
	defer func() { _ = file.Close() }()

	writer := csv.NewWriter(file)
	if errors.Is(statErr, os.ErrNotExist) {
		if err := writer.Write(columns); err != nil {
			return fmt.Errorf("write header: %w", err)
		}
	}
	if err := writer.Write(row.record()); err != nil {
		return fmt.Errorf("write row: %w", err)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush results: %w", err)
	}
	r.done[row.CycleID] = struct{}{}
	return file.Sync()
}

func readRows(path string) ([]map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open results: %w", err)
	}
	defer func() { _ = file.Close() }()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if !slices.Equal(header, columns) {
		return nil, fmt.Errorf("%s has a different header", path)
	}
	var rows []map[string]string
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		row := make(map[string]string, len(columns))
		for i, column := range columns {
			row[column] = record[i]
		}
		rows = append(rows, row)
	}
}

// Merge combines per-laptop files into target. It refuses to mix commits or
// image versions, and refuses a cycle that appears twice.
func Merge(sources []string, target string) (int, error) {
	if len(sources) == 0 {
		return 0, errors.New("no result files to merge")
	}
	var merged [][]string
	seen := make(map[string]string)
	commit, versions := "", ""
	for _, source := range sources {
		rows, err := readRows(source)
		if err != nil {
			return 0, err
		}
		for _, row := range rows {
			if commit == "" {
				commit, versions = row["git_commit"], row["versions_json"]
			}
			if row["git_commit"] != commit {
				return 0, fmt.Errorf("%s: commit %s differs from %s", source, row["git_commit"], commit)
			}
			if row["versions_json"] != versions {
				return 0, fmt.Errorf("%s: image versions differ for cycle %s", source, row["cycle_id"])
			}
			if other, dup := seen[row["cycle_id"]]; dup {
				return 0, fmt.Errorf("cycle %s is in both %s and %s", row["cycle_id"], other, source)
			}
			seen[row["cycle_id"]] = source
			record := make([]string, len(columns))
			for i, column := range columns {
				record[i] = row[column]
			}
			merged = append(merged, record)
		}
	}
	file, err := os.Create(target)
	if err != nil {
		return 0, fmt.Errorf("create %s: %w", target, err)
	}
	defer func() { _ = file.Close() }()
	writer := csv.NewWriter(file)
	if err := writer.WriteAll(append([][]string{columns}, merged...)); err != nil {
		return 0, fmt.Errorf("write %s: %w", target, err)
	}
	return len(merged), nil
}

// assertionFailure returns a description of the first violated assert, or "".
func assertionFailure(cycle Cycle, res checker.Result) string {
	values := map[string]int{
		"lost":         res.Lost,
		"ghost":        res.Ghost,
		"duplicates":   res.Duplicates,
		"out_of_order": res.OutOfOrder,
	}
	if cycle.AssertScope == "hooked" {
		values = map[string]int{"lost": res.HookedLost, "ghost": res.HookedGhost, "duplicates": res.HookedDup}
	}
	for _, damage := range res.Business {
		values["business_damage"] = max(values["business_damage"], damage.Total())
	}
	for _, metric := range sortedKeys(cycle.Assert) {
		if got, ok := values[metric]; ok && got != cycle.Assert[metric] {
			return fmt.Sprintf("%s = %d, want %d", metric, got, cycle.Assert[metric])
		}
	}
	return ""
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// Summary describes a results file.
type Summary struct {
	Rows     int
	Statuses map[string]int
	Commits  []string
	Versions int
	Missing  []string // catalogue cycles without a row
	Problems []string // anything that makes the file unusable for the paper
}

// Verify checks a results file against the expected cycles.
func Verify(path string, expected []Cycle) (Summary, error) {
	rows, err := readRows(path)
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{Rows: len(rows), Statuses: make(map[string]int)}
	seen := make(map[string]int)
	commits := make(map[string]struct{})
	versions := make(map[string]struct{})
	for _, row := range rows {
		summary.Statuses[row["status"]]++
		seen[row["cycle_id"]]++
		commits[row["git_commit"]] = struct{}{}
		versions[row["versions_json"]] = struct{}{}
	}
	summary.Commits = slices.Sorted(maps.Keys(commits))
	summary.Versions = len(versions)
	for _, commit := range summary.Commits {
		if strings.HasSuffix(commit, "-dirty") {
			summary.Problems = append(summary.Problems, "rows from a dirty tree: "+commit)
		}
	}
	if len(summary.Commits) > 1 {
		summary.Problems = append(summary.Problems, fmt.Sprintf("%d commits in one file", len(summary.Commits)))
	}
	if summary.Versions > 1 {
		summary.Problems = append(summary.Problems, fmt.Sprintf("%d image version sets in one file", summary.Versions))
	}
	for _, id := range slices.Sorted(maps.Keys(seen)) {
		if seen[id] > 1 {
			summary.Problems = append(summary.Problems, "cycle appears twice: "+id)
		}
	}
	for _, cycle := range expected {
		if seen[cycle.ID] == 0 {
			summary.Missing = append(summary.Missing, cycle.ID)
		}
	}
	return summary, nil
}
