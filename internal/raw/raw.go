// Package raw holds the per-cycle raw files of SPEC 11.5: everything the checker
// reads, written by the runner before the next cycle's RESET deletes it, so every
// metric can be recomputed from the files alone.
package raw

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// File names inside raw/<cycle_id>/.
const (
	TruthFile    = "truth.json.gz"
	OutboxFile   = "outbox.json.gz"
	ConsumerFile = "consumer.json.gz"
	LoadFile     = "load.json.gz"
	MarkersFile  = "markers.log.gz"
	TimelineFile = "timeline.json"
)

// Order is one row of orders in the truth database.
type Order struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
	Version int    `json:"version"`
	Amount  string `json:"amount"` // NUMERIC(12,2) as text, exact
}

// History is one row of orders_history with its transaction's commit time.
type History struct {
	OrderID    string     `json:"order_id"`
	Version    int        `json:"version"`
	Status     string     `json:"status"`
	XMin       uint32     `json:"xmin"`
	CommitTime *time.Time `json:"commit_time"` // nil when Postgres returned NULL
}

// Truth is the truth database's state at CHECK (SPEC 11.1).
type Truth struct {
	Source  string    `json:"truth_source"` // primary | promoted_replica
	XIDAge  int64     `json:"xid_age"`
	Orders  []Order   `json:"orders"`
	History []History `json:"history"`
}

// OutboxRow is one row of outbox (P3, P4).
type OutboxRow struct {
	ID          string     `json:"id"`
	AggregateID string     `json:"aggregateid"`
	Type        string     `json:"type"`
	Seq         int        `json:"seq"`
	CreatedAt   time.Time  `json:"created_at"`
	SentAt      *time.Time `json:"sent_at"`
}

// Payment is one row of a consumer.payments_<strategy> table.
type Payment struct {
	PaymentID int64  `json:"payment_id"`
	OrderID   string `json:"order_id"`
	EventKey  string `json:"event_key"`
	Amount    string `json:"amount"` // NUMERIC(12,2) as text, exact
}

// Consumer is the consumer schema's state at CHECK (group G).
type Consumer struct {
	Payments  map[string][]Payment `json:"payments"` // by strategy
	Processed []string             `json:"processed_events"`
}

// Request is one load-generator request.
type Request struct {
	StartMS   float64 `json:"start_ms"` // since Load.Start
	LatencyMS float64 `json:"latency_ms"`
	OK        bool    `json:"ok"`
}

// Load is every request the load generator issued.
type Load struct {
	Start    time.Time `json:"start"`
	Requests []Request `json:"requests"`
}

// CPUSample is a reading of a container's cumulative CPU time.
type CPUSample struct {
	At      time.Time `json:"at"`
	TotalNS uint64    `json:"total_ns"`
}

// Sample is a timed numeric reading, such as WAL retained or outbox backlog.
type Sample struct {
	At    time.Time `json:"at"`
	Value int64     `json:"value"`
}

// FaultEvent is one fault's inject and recover times.
type FaultEvent struct {
	Type      string    `json:"type"`
	Target    string    `json:"target"`
	Injected  time.Time `json:"injected"`
	Recovered time.Time `json:"recovered"`
}

// Timeline is everything the runner observed about the cycle's timing and
// state, other than the data in the other files.
type Timeline struct {
	CycleID   string                  `json:"cycle_id"`
	Pattern   string                  `json:"pattern"`
	Seed      uint64                  `json:"seed"`
	Env       map[string]string       `json:"env"`
	Consumers []string                `json:"consumers"`
	States    map[string][2]time.Time `json:"states"` // state name -> start, end

	LoadStart     time.Time    `json:"load_start"`
	WarmupEnd     time.Time    `json:"warmup_end"`
	Faults        []FaultEvent `json:"faults"`
	PointHit      time.Time    `json:"point_hit"` // crash or stall point, when it fired
	FaultEnd      time.Time    `json:"fault_end"` // zero for cycles without a fault
	LoadStop      time.Time    `json:"load_stop"`
	ObservedUntil time.Time    `json:"observed_until"`
	FailoverAt    time.Time    `json:"failover_at"`

	CPUStart CPUSample `json:"cpu_start"` // postgres-primary, at warm-up end
	CPUEnd   CPUSample `json:"cpu_end"`   // at load stop
	CPUValid bool      `json:"cpu_valid"` // both samples read from the same container

	WALRetained   []Sample `json:"wal_retained"`
	OutboxBacklog []Sample `json:"outbox_backlog"`
	Slots         []Slot   `json:"slots"`

	MarkerIDs      []string          `json:"drain_marker_ids"`
	MarkerOffsets  map[string]string `json:"drain_marker_offsets"` // marker ID -> "partition:offset"
	DrainTargetLSN string            `json:"drain_target_lsn"`
	DrainSlotLSN   string            `json:"drain_slot_lsn"`
	DrainReason    string            `json:"drain_reason"`
	ConnectStatus  json.RawMessage   `json:"connect_status,omitempty"`
}

// Slot is one reading of a replication slot.
type Slot struct {
	At             time.Time `json:"at"`
	Name           string    `json:"slot"`
	Active         bool      `json:"active"`
	ConfirmedFlush string    `json:"confirmed_flush_lsn"`
	Restart        string    `json:"restart_lsn"`
	CurrentWAL     string    `json:"current_wal_lsn"`
}

// Dir is a cycle's raw directory inside a session directory.
func Dir(sessionDir, cycleID string) string { return filepath.Join(sessionDir, "raw", cycleID) }

// DumpPath is a cycle's topic dump inside a session directory.
func DumpPath(sessionDir, cycleID string) string {
	return filepath.Join(sessionDir, "dumps", cycleID+".jsonl.gz")
}

// WriteJSON stores v as JSON, gzipped when name ends in .gz, atomically.
func WriteJSON(dir, name string, v any) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, name)
	tmp := path + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close(); _ = os.Remove(tmp) }()
	var w io.Writer = file
	var zw *gzip.Writer
	if filepath.Ext(name) == ".gz" {
		zw = gzip.NewWriter(file)
		w = zw
	}
	buf := bufio.NewWriter(w)
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		return fmt.Errorf("encode %s: %w", name, err)
	}
	errs := []error{buf.Flush()}
	if zw != nil {
		errs = append(errs, zw.Close())
	}
	if err := errors.Join(append(errs, file.Sync(), file.Close())...); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadJSON loads a file written by WriteJSON into v.
func ReadJSON(dir, name string, v any) error {
	file, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	var r io.Reader = file
	if filepath.Ext(name) == ".gz" {
		zr, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		r = zr
	}
	if err := json.NewDecoder(r).Decode(v); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
	}
	return nil
}

// WriteBytes stores data gzipped, atomically.
func WriteBytes(dir, name string, data []byte) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, name)
	tmp := path + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close(); _ = os.Remove(tmp) }()
	zw := gzip.NewWriter(file)
	if _, err := zw.Write(data); err != nil {
		return err
	}
	if err := errors.Join(zw.Close(), file.Sync(), file.Close()); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadBytes loads a file written by WriteBytes.
func ReadBytes(dir, name string) ([]byte, error) {
	file, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	zr, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return io.ReadAll(zr)
}
