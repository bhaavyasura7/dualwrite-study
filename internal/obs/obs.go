// Package obs sets up structured logging and names the evidence markers (SPEC 13).
package obs

import (
	"log/slog"
	"math"
	"os"
)

// Log markers the checker and runner search for.
const (
	CrashPointHit = "CRASH_POINT_HIT"
	StallPointHit = "STALL_POINT_HIT"
	StallPointEnd = "STALL_POINT_END"
	ProduceTrace  = "PRODUCE_REQUEST"
	SentCalled    = "SENT_CALLED"
	SendAcked     = "SEND_ACKED"
	SendFailed    = "SEND_FAILED"
)

// Setup installs the JSON logger with the fields required on every line.
func Setup(component, pattern, cycleID string) {
	handler := slog.NewJSONHandler(os.Stdout, nil)
	slog.SetDefault(slog.New(handler).With("component", component, "pattern", pattern, "cycle_id", cycleID))
}

// Percentile returns the nearest-rank q-th percentile of sorted, or 0 if empty.
func Percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(q / 100 * float64(len(sorted))))
	return sorted[max(rank, 1)-1]
}
