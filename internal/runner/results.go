package runner

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"dualwrite-study/internal/checker"
)

// columns is the results CSV header (SPEC 14.3).
var columns = []string{
	"run_id", "cycle_id", "vector_id", "pattern", "mode", "fault", "crash_point", "repeat", "seed",
	"status", "sweep_json", "env_json", "versions_json", "load_rate", "orders_total", "events_total",
	"lost", "ghost", "duplicates", "out_of_order", "stale_redelivery", "lost_after_send",
	"hooked_lost", "hooked_dup", "collateral_lost", "collateral_dup",
	"business_damage_naive", "business_damage_dedup", "business_damage_upsert",
	"wal_retained_bytes_max", "outbox_backlog_max", "relay_batches", "batch_rows_mean", "batch_rows_max",
	"send_failed", "crash_after_n", "inflight_at_crash", "oldest_pending_ms",
	"failover_at", "failover_lag_bytes", "failover_lag_ms", "host", "shard", "git_commit",
	"window_p50_ms", "window_p99_ms", "window_max_ms", "recovery_s",
	"throughput_ops", "latency_p50_ms", "latency_p99_ms", "db_cpu_pct", "load_s", "load_failed",
	"pre_host_load", "pre_db_cpu_pct", "pre_p99_ms", "pre_flag", "started_at", "finished_at",
}

// Row is one cycle's result.
type Row struct {
	RunID, CycleID, VectorID, Pattern, Mode, Fault, CrashPoint string
	Repeat                                                     int
	Seed                                                       uint64
	Status, SweepJSON, EnvJSON, VersionsJSON                   string
	RecoveryS                                                  string
	LoadFailed                                                 int
	FailoverLagBytes, FailoverLagMS                            string
	FailoverAt                                                 time.Time
	LoadRate                                                   int
	Result                                                     checker.Result
	CrashAfterN                                                int
	Host, Shard, GitCommit                                     string
	ThroughputOps, LatencyP50MS, LatencyP99MS                  float64
	DBCPUPct, LoadS                                            float64
	PreHostLoad, PreDBCPUPct, PreP99MS                         float64
	PreFlag                                                    string
	WALRetainedMax                                             int64
	OutboxBacklogMax                                           int64
	StartedAt, FinishedAt                                      time.Time
}

func (r Row) record() []string {
	number := func(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
	count := strconv.Itoa
	damage := func(strategy string) string {
		if v, ok := r.Result.BusinessDamage[strategy]; ok {
			return count(v)
		}
		return ""
	}
	wal := ""
	if r.WALRetainedMax > 0 {
		wal = strconv.FormatInt(r.WALRetainedMax, 10)
	}
	failoverAt := ""
	if !r.FailoverAt.IsZero() {
		failoverAt = r.FailoverAt.UTC().Format(time.RFC3339Nano)
	}
	backlog := ""
	if r.Pattern == "P3" {
		backlog = strconv.FormatInt(r.OutboxBacklogMax, 10)
	}
	res := r.Result
	return []string{
		r.RunID, r.CycleID, r.VectorID, r.Pattern, r.Mode, r.Fault, r.CrashPoint, count(r.Repeat),
		strconv.FormatUint(r.Seed, 10),
		r.Status, r.SweepJSON, r.EnvJSON, r.VersionsJSON, count(r.LoadRate), count(res.OrdersTotal), count(res.EventsTotal),
		count(res.Lost), count(res.Ghost), count(res.Duplicates), count(res.OutOfOrder), count(res.StaleRedelivery), count(res.LostAfterSend),
		count(res.HookedLost), count(res.HookedDup), count(res.CollateralLost), count(res.CollateralDup),
		damage("naive"), damage("dedup"), damage("upsert"),
		wal, backlog, count(res.RelayBatches), number(res.BatchRowsMean), count(res.BatchRowsMax),
		count(res.SendFailed), count(r.CrashAfterN), count(res.InflightAtCrash), number(res.OldestPendingMS),
		failoverAt, r.FailoverLagBytes, r.FailoverLagMS, r.Host, r.Shard, r.GitCommit,
		number(res.WindowP50MS), number(res.WindowP99MS), number(res.WindowMaxMS), r.RecoveryS,
		number(r.ThroughputOps), number(r.LatencyP50MS), number(r.LatencyP99MS), number(r.DBCPUPct), number(r.LoadS), count(r.LoadFailed),
		number(r.PreHostLoad), number(r.PreDBCPUPct), number(r.PreP99MS), r.PreFlag,
		r.StartedAt.UTC().Format(time.RFC3339), r.FinishedAt.UTC().Format(time.RFC3339),
	}
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
	for _, damage := range res.BusinessDamage {
		values["business_damage"] = max(values["business_damage"], damage)
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
