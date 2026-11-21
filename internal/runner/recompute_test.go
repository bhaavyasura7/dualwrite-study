package runner

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dualwrite-study/internal/checker"
	"dualwrite-study/internal/dump"
	"dualwrite-study/internal/raw"
)

// writeCycle builds one P3 group-G cycle's raw files and dump, with a fault,
// a lost version, a duplicate payment and load data, as the runner would.
func writeCycle(t *testing.T, session, id string) {
	t.Helper()
	t0 := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	ms := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Millisecond) }
	ptr := func(t time.Time) *time.Time { return &t }
	a, b := "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"
	truth := raw.Truth{Source: "primary", XIDAge: 1000,
		Orders: []raw.Order{{OrderID: a, Status: "PAID", Version: 2, Amount: "499.00"}, {OrderID: b, Status: "PLACED", Version: 1, Amount: "499.00"}},
		History: []raw.History{
			{OrderID: a, Version: 1, Status: "PLACED", CommitTime: ptr(ms(11_000))},
			{OrderID: a, Version: 2, Status: "PAID", CommitTime: ptr(ms(12_000))},
			{OrderID: b, Version: 1, Status: "PLACED", CommitTime: ptr(ms(25_000))}, // never delivered
		}}
	value := func(event, order string, seq int, status string) []byte {
		return []byte(`{"event_id":"` + event + `","order_id":"` + order + `","seq":` + string(rune('0'+seq)) + `,"status":"` + status + `"}`)
	}
	records := []dump.Record{
		{Partition: 0, Offset: 0, TimestampMS: ms(11_020).UnixMilli(), TimestampType: 1, Value: value("aaaaaaaa-0000-0000-0000-000000000001", a, 1, "PLACED")},
		{Partition: 0, Offset: 1, TimestampMS: ms(12_030).UnixMilli(), TimestampType: 1, Value: value("aaaaaaaa-0000-0000-0000-000000000002", a, 2, "PAID")},
	}
	consumer := raw.Consumer{Payments: map[string][]raw.Payment{
		"naive":  {{PaymentID: 1, OrderID: a, EventKey: "aaaaaaaa-0000-0000-0000-000000000002", Amount: "499.00"}, {PaymentID: 2, OrderID: a, EventKey: "aaaaaaaa-0000-0000-0000-000000000002", Amount: "499.00"}},
		"dedup":  {{PaymentID: 1, OrderID: a, EventKey: "aaaaaaaa-0000-0000-0000-000000000002", Amount: "499.00"}},
		"upsert": {{PaymentID: 1, OrderID: a, EventKey: "aaaaaaaa-0000-0000-0000-000000000002", Amount: "499.00"}},
	}}
	var requests []raw.Request
	for n := 0; n < 60_000; n += 10 {
		requests = append(requests, raw.Request{StartMS: float64(n), LatencyMS: 2, OK: n < 20_000 || n >= 33_000})
	}
	timeline := raw.Timeline{CycleID: id, Pattern: "P3", Consumers: []string{"naive", "dedup", "upsert"},
		LoadStart: t0, WarmupEnd: ms(10_000), FaultEnd: ms(30_000), LoadStop: ms(60_000), ObservedUntil: ms(70_000),
		Faults:   []raw.FaultEvent{{Type: "crash", Injected: ms(20_000), Recovered: ms(30_000)}},
		CPUStart: raw.CPUSample{At: ms(10_000), TotalNS: 1e9}, CPUEnd: raw.CPUSample{At: ms(60_000), TotalNS: 6e9}, CPUValid: true,
	}
	dir := raw.Dir(session, id)
	for _, err := range []error{
		raw.WriteJSON(dir, raw.TruthFile, truth),
		raw.WriteJSON(dir, raw.ConsumerFile, consumer),
		raw.WriteJSON(dir, raw.LoadFile, raw.Load{Start: t0, Requests: requests}),
		raw.WriteBytes(dir, raw.MarkersFile, nil),
		raw.WriteJSON(dir, raw.TimelineFile, timeline),
		dump.WriteForTest(raw.DumpPath(session, id), records),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestRecomputeReproducesResults is go criterion 15 in miniature: the row the
// runner writes from a cycle's raw files is reproduced exactly from them.
func TestRecomputeReproducesResults(t *testing.T) {
	session := t.TempDir()
	writeCycle(t, session, "G-1_P3_r01")
	loaded, err := checker.FromRaw(session, "G-1_P3_r01")
	if err != nil {
		t.Fatal(err)
	}
	r := row("G-1_P3_r01", "abc")
	r.Pattern, r.Status, r.Consumers = "P3", "OK", true
	r.Result, r.Recoveries, r.Cost, _ = Evaluate(loaded)
	// The cycle exercises the new metrics, so a silent zero would not pass.
	if r.Result.Lost != 1 || r.Result.Business["naive"].Duplicate != 1 || r.Result.Business["upsert"].Missing != 0 ||
		!r.Recoveries.Consistency.Known || r.Recoveries.Consistency.Recovered || !r.Recoveries.Service.Recovered ||
		r.Recoveries.Service.Seconds != 3 || !r.Cost.Known || r.Cost.CommittedChanges != 3 {
		t.Fatalf("unexpected metrics: %+v %+v %+v", r.Result, r.Recoveries, r.Cost)
	}
	results := &Results{path: filepath.Join(session, "results.csv"), done: map[string]struct{}{}}
	if err := results.Append(r); err != nil {
		t.Fatal(err)
	}
	report, err := Recompute(session, "def")
	if err != nil {
		t.Fatal(err)
	}
	if report.Recomputed != 1 || len(report.Differences) != 0 {
		t.Fatalf("report %+v", report)
	}

	// A changed value in results.csv is reported, and results.csv itself is untouched.
	original, err := os.ReadFile(results.path)
	if err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(string(original), ",OK,", ",OK,", 1)
	recs, err := csv.NewReader(strings.NewReader(tampered)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range columns {
		if c == "lost" {
			recs[1][i] = "7"
		}
	}
	file, err := os.Create(results.path)
	if err != nil {
		t.Fatal(err)
	}
	if err := csv.NewWriter(file).WriteAll(recs); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	report, err = Recompute(session, "def")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Differences) != 1 || !strings.Contains(report.Differences[0], `lost: was "7", now "1"`) {
		t.Fatalf("differences %q", report.Differences)
	}
	recomputed, err := os.ReadFile(filepath.Join(session, "results.recomputed.csv"))
	if err != nil || !strings.Contains(strings.SplitN(string(recomputed), "\n", 2)[0], "analysis_commit") {
		t.Fatalf("recomputed file: %v", err)
	}
}
