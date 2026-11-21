package runner

import (
	"strings"
	"testing"
	"time"

	"dualwrite-study/internal/checker"
)

func TestMarkerID(t *testing.T) {
	seen := map[string]bool{}
	for n := range maxMarkers {
		id := markerID(42, n)
		if id.Version() != 4 || id.Variant().String() != "RFC4122" {
			t.Errorf("marker %d: %s is not a version 4 UUID", n, id)
		}
		if seen[id.String()] {
			t.Fatalf("marker %d repeats %s", n, id)
		}
		seen[id.String()] = true
		if markerID(42, n) != id {
			t.Fatal("marker IDs must be reproducible from the seed")
		}
	}
	if markerID(43, 0) == markerID(42, 0) {
		t.Error("different seeds gave the same marker")
	}
}

func TestConnectorsRunning(t *testing.T) {
	for doc, want := range map[string]bool{
		`{"p4-x":{"status":{"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"}]}}}`:    true,
		`{"p4-x":{"status":{"connector":{"state":"RUNNING"},"tasks":[{"state":"FAILED"}]}}}`:     false,
		`{"p4-x":{"status":{"connector":{"state":"RUNNING"},"tasks":[]}}}`:                       false,
		`{"p4-x":{"status":{"connector":{"state":"UNASSIGNED"},"tasks":[{"state":"RUNNING"}]}}}`: false,
		`{}`:       false,
		`not json`: false,
	} {
		if got := connectorsRunning([]byte(doc)); got != want {
			t.Errorf("%s: got %v", doc, got)
		}
	}
}

func TestSameRecords(t *testing.T) {
	ts := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	a := []checker.Record{
		{Pair: checker.Pair{OrderID: "o", Seq: 1}, EventID: "e1", Partition: 0, Offset: 0, Timestamp: ts},
		{Pair: checker.Pair{OrderID: "p", Seq: 1}, EventID: "e2", Partition: 1, Offset: 0, Timestamp: ts},
	}
	reordered := []checker.Record{a[1], a[0]}
	if err := sameRecords(a, reordered); err != nil {
		t.Fatalf("order of reading must not matter: %v", err)
	}
	if err := sameRecords(a, a[:1]); err == nil || !strings.Contains(err.Error(), "1 committed records") {
		t.Fatalf("missing record: %v", err)
	}
	changed := []checker.Record{a[0], a[1]}
	changed[1].Timestamp = ts.Add(time.Millisecond)
	if err := sameRecords(a, changed); err == nil {
		t.Fatal("a changed timestamp passed")
	}
}

// TestRecordHasEveryColumn guards the CSV: every column has a value, metric
// columns come from MetricFields, and empty-by-design columns stay empty.
func TestRecordHasEveryColumn(t *testing.T) {
	r := row("a", "abc")
	r.Pattern = "P4"
	record := r.record()
	if len(record) != len(columns) {
		t.Fatalf("%d fields, %d columns", len(record), len(columns))
	}
	value := map[string]string{}
	for i, c := range columns {
		value[c] = record[i]
	}
	for _, c := range []string{"lost_after_ack", "business_damage_naive", "missing_payments_dedup", "service_recovery_s",
		"consistency_recovered", "db_cpu_s", "window_p99_pre_ms", "failover_at", "xid_age"} {
		if value[c] != "" {
			t.Errorf("%s = %q, want empty", c, value[c])
		}
	}
	if value["window_valid"] != "true" || value["truth_source"] != "" {
		t.Errorf("window_valid %q, truth_source %q", value["window_valid"], value["truth_source"])
	}
	seen := map[string]bool{}
	for _, c := range columns {
		if seen[c] {
			t.Errorf("column %s twice", c)
		}
		seen[c] = true
	}
	for _, c := range MetricColumns {
		if !seen[c] {
			t.Errorf("metric column %s not in the header", c)
		}
	}
}
