package runner

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"dualwrite-study/internal/checker"
)

// checkedStatuses are the statuses of cycles whose CHECK ran, so their metric
// columns come from the raw files and must recompute exactly. Other cycles keep
// their row as it is.
var checkedStatuses = []string{"OK", "CONTROL_FAIL"}

// RecomputeReport summarises a recomputation (SPEC 11.5).
type RecomputeReport struct {
	Rows, Recomputed, Skipped int
	Differences               []string // "cycle column: was X, now Y"
}

// Recompute rewrites every metric column of a session directory from its raw
// files alone and writes results.recomputed.csv beside results.csv, with
// analysisCommit in a final column. results.csv is never changed.
func Recompute(sessionDir, analysisCommit string) (RecomputeReport, error) {
	var report RecomputeReport
	rows, err := readRows(filepath.Join(sessionDir, "results.csv"))
	if err != nil {
		return report, err
	}
	out := [][]string{append(slices.Clone(columns), "analysis_commit")}
	for _, row := range rows {
		report.Rows++
		id := row["cycle_id"]
		if slices.Contains(checkedStatuses, row["status"]) {
			loaded, err := checker.FromRaw(sessionDir, id)
			if err != nil {
				return report, fmt.Errorf("%s: %w", id, err)
			}
			res, rec, cost, _ := Evaluate(loaded)
			fields := MetricFields(res, rec, cost, row["pattern"], len(loaded.Timeline.Consumers) > 0)
			for _, column := range MetricColumns {
				if fields[column] != row[column] {
					report.Differences = append(report.Differences, fmt.Sprintf("%s %s: was %q, now %q", id, column, row[column], fields[column]))
				}
				row[column] = fields[column]
			}
			report.Recomputed++
		} else {
			report.Skipped++
		}
		record := make([]string, 0, len(columns)+1)
		for _, column := range columns {
			record = append(record, row[column])
		}
		out = append(out, append(record, analysisCommit))
	}
	path := filepath.Join(sessionDir, "results.recomputed.csv")
	file, err := os.Create(path)
	if err != nil {
		return report, err
	}
	writer := csv.NewWriter(file)
	if err := writer.WriteAll(out); err != nil {
		_ = file.Close()
		return report, err
	}
	return report, errors.Join(file.Sync(), file.Close())
}
