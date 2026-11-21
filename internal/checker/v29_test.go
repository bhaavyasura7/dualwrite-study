package checker

import (
	"path/filepath"
	"testing"
	"time"

	"dualwrite-study/internal/dump"
	"dualwrite-study/internal/raw"
)

var t0 = time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

// TestMarkersExcluded: drain markers in DB, Kafka and payments change nothing (SPEC 11.4).
func TestMarkersExcluded(t *testing.T) {
	base := Input{
		DB:    []Version{{Pair: Pair{"a", 1}, CommitTime: at(0)}},
		Kafka: []Record{{Pair: Pair{"a", 1}, Timestamp: at(10)}},
	}
	withMarker := base
	withMarker.DB = append(withMarker.DB, Version{Pair: Pair{"m", 1}, Status: "MARKER", CommitTime: at(5)})
	withMarker.Kafka = append(withMarker.Kafka, Record{Pair: Pair{"m", 1}, Timestamp: at(9)}, Record{Pair: Pair{"m", 1}, Timestamp: at(9)})
	withMarker.Orders = []Order{{OrderID: "m", Status: "MARKER"}}
	withMarker.Payments = map[string][]Payment{"naive": {{PaymentID: 1, OrderID: "m"}}}
	withMarker.MarkerIDs = []string{"m"}
	base.Payments = map[string][]Payment{"naive": {}}

	want, _ := Check(base)
	got, _ := Check(withMarker)
	if got.EventsTotal != want.EventsTotal || got.Duplicates != 0 || got.Ghost != 0 || got.WindowMaxMS != want.WindowMaxMS ||
		got.Business["naive"] != want.Business["naive"] {
		t.Fatalf("markers changed the result:\n got %+v\nwant %+v", got, want)
	}
}

// TestCommitTimeSafeguards: a missing or impossible commit time empties the window (SPEC 5.1).
func TestCommitTimeSafeguards(t *testing.T) {
	in := Input{
		DB: []Version{
			{Pair: Pair{"a", 1}, CommitTime: at(0)},
			{Pair: Pair{"b", 1}, CommitMissing: true},
			{Pair: Pair{"c", 1}, CommitTime: at(100)},
		},
		Kafka: []Record{
			{Pair: Pair{"a", 1}, Timestamp: at(20)},
			{Pair: Pair{"b", 1}, Timestamp: at(30)},
			{Pair: Pair{"c", 1}, Timestamp: at(90)}, // 10 ms before its commit
		},
	}
	got, damage := Check(in)
	if got.CommitTSMissing != 1 || got.WindowNegative != 1 || !got.WindowInvalid || got.WindowP50MS != 0 || got.WindowMaxMS != 0 {
		t.Fatalf("got %+v", got)
	}
	if len(damage.CommitTSMissing) != 1 || damage.CommitTSMissing[0] != (Pair{"b", 1}) || damage.WindowNegative[0] != (Pair{"c", 1}) {
		t.Fatalf("damage %+v / %+v", damage.CommitTSMissing, damage.WindowNegative)
	}
	// Within the 5 ms tolerance is not negative.
	in.DB, in.Kafka = in.DB[:1], []Record{{Pair: Pair{"a", 1}, Timestamp: at(-4)}}
	if got, _ := Check(in); got.WindowNegative != 0 || got.WindowInvalid {
		t.Fatalf("4 ms early counted as impossible: %+v", got)
	}
}

func timing() Timing {
	return Timing{
		LoadStart: t0, WarmupEnd: at(10_000), FirstFault: at(20_000), FaultEnd: at(30_000),
		LoadStop: at(60_000), ObservedUntil: at(70_000),
	}
}

func TestConsistencyRecovery(t *testing.T) {
	in := Input{
		DB:    []Version{{Pair: Pair{"a", 1}, CommitTime: at(15_000)}, {Pair: Pair{"b", 1}, CommitTime: at(25_000)}},
		Kafka: []Record{{Pair: Pair{"a", 1}, Timestamp: at(15_010)}, {Pair: Pair{"b", 1}, Timestamp: at(33_000)}},
	}
	got := ComputeRecoveries(in, timing(), nil, false).Consistency
	if !got.Known || !got.Recovered || got.Seconds != 3 {
		t.Fatalf("got %+v", got)
	}
	// SPEC 11.4: a P1 cycle with one version never delivered is censored.
	in.Kafka = in.Kafka[:1]
	got = ComputeRecoveries(in, timing(), nil, false).Consistency
	if !got.Known || got.Recovered || got.Seconds != 40 {
		t.Fatalf("lost version: got %+v, want censored at 40 s", got)
	}
	if r := ComputeRecoveries(in, timing(), nil, true); r.Consistency.Known || r.Synchronization.Known {
		t.Fatalf("invalid window must leave commit-time recoveries unknown: %+v", r)
	}
	if r := ComputeRecoveries(in, Timing{}, nil, false); r.Service.Known || r.Consistency.Known {
		t.Fatalf("no fault must leave all unknown: %+v", r)
	}
}

// requests builds one request per 10 ms from second from to second to, failing
// in the seconds listed.
func requests(from, to int, failing map[int]bool) []raw.Request {
	var out []raw.Request
	for ms := from * 1000; ms < to*1000; ms += 10 {
		out = append(out, raw.Request{StartMS: float64(ms), OK: !failing[ms/1000]})
	}
	return out
}

func TestServiceRecovery(t *testing.T) {
	// SPEC 11.4: the service fails for 7 s after the fault ends (30-36), so the
	// first full healthy 5 s stretch starts at 37.
	failing := map[int]bool{}
	for s := 25; s <= 36; s++ {
		failing[s] = true
	}
	got := ComputeRecoveries(Input{}, timing(), requests(0, 60, failing), true).Service
	if !got.Known || !got.Recovered || got.Seconds != 7 {
		t.Fatalf("got %+v, want 7 s", got)
	}
	// Healthy from the fault's end: 0.
	got = ComputeRecoveries(Input{}, timing(), requests(0, 60, nil), true).Service
	if !got.Recovered || got.Seconds != 0 {
		t.Fatalf("never degraded: got %+v", got)
	}
	// Failing until 57: no 5 s stretch before load stop at 60; censored at 30 s.
	for s := 37; s <= 57; s++ {
		failing[s] = true
	}
	got = ComputeRecoveries(Input{}, timing(), requests(0, 60, failing), true).Service
	if got.Recovered || got.Seconds != 30 {
		t.Fatalf("censoring: got %+v", got)
	}
}

func TestSynchronizationRecovery(t *testing.T) {
	var in Input
	add := func(name string, commitMS, windowMS int, delivered bool) {
		in.DB = append(in.DB, Version{Pair: Pair{name, 1}, CommitTime: at(commitMS)})
		if delivered {
			in.Kafka = append(in.Kafka, Record{Pair: Pair{name, 1}, Timestamp: at(commitMS + windowMS)})
		}
	}
	add("pre", 15_000, 50, true)     // baseline p99 = 50 ms; limit 1,050 ms
	add("slow", 33_500, 5_000, true) // late
	add("ok1", 40_000, 60, true)
	add("ok2", 55_000, 40, true)
	got := ComputeRecoveries(in, timing(), nil, false)
	if !got.WindowP99PreKnown || got.WindowP99PreMS != 50 {
		t.Fatalf("baseline %+v", got)
	}
	// Last late version at 33.5 s, so recovery at the next whole second after the fault: 34 s, 4 s after.
	if s := got.Synchronization; !s.Recovered || s.Seconds != 4 {
		t.Fatalf("got %+v, want 4 s", s)
	}
	// A lost version at 52 s leaves under 10 s before load stop: censored.
	add("lost", 52_000, 0, false)
	if s := ComputeRecoveries(in, timing(), nil, false).Synchronization; s.Recovered || s.Seconds != 30 {
		t.Fatalf("got %+v, want censored at 30 s", s)
	}
}

func TestCost(t *testing.T) {
	in := Input{DB: []Version{
		{Pair: Pair{"a", 1}, CommitTime: at(1_000)},
		{Pair: Pair{"a", 2}, CommitTime: at(2_000)},
		{Pair: Pair{"b", 1}, CommitTime: at(3_000)},
		{Pair: Pair{"c", 1}, CommitTime: at(12_000)}, // after the interval
	}}
	start := raw.CPUSample{At: at(0), TotalNS: 1_000_000_000}
	end := raw.CPUSample{At: at(10_000), TotalNS: 4_000_000_000}
	c := ComputeCost(in, start, end, true, false)
	if !c.Known || c.DBCPUS != 3 || c.CommittedChanges != 3 || c.WindowS != 10 || c.PerThousand != 1000 ||
		c.ChangesPerS != 0.3 || c.OrdersPerS != 0.2 || c.DBCPUPct != 30 {
		t.Fatalf("got %+v", c)
	}
	if ComputeCost(in, start, end, false, false).Known || ComputeCost(in, start, end, true, true).Known {
		t.Fatal("cost must be unknown without valid samples or commit times")
	}
}

func TestParseCents(t *testing.T) {
	for in, want := range map[string]int64{"499.00": 49900, "499": 49900, "0.5": 50, "-1.25": -125, "12.3": 1230} {
		if got, err := ParseCents(in); err != nil || got != want {
			t.Errorf("%s: %d, %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "1.234", "x", "1.x"} {
		if _, err := ParseCents(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

// TestFromRawMatchesCheck writes a cycle's raw files and dump and checks that
// FromRaw rebuilds the same input the live path would build (SPEC 11.5).
func TestFromRawMatchesCheck(t *testing.T) {
	session := t.TempDir()
	commit := at(1_000)
	truth := raw.Truth{Source: "primary", History: []raw.History{
		{OrderID: "11111111-1111-1111-1111-111111111111", Version: 1, Status: "PLACED", CommitTime: &commit},
	}, Orders: []raw.Order{{OrderID: "11111111-1111-1111-1111-111111111111", Status: "PLACED", Version: 1, Amount: "499.00"}}}
	dir := raw.Dir(session, "X_P1_r01")
	value := []byte(`{"event_id":"22222222-2222-2222-2222-222222222222","order_id":"11111111-1111-1111-1111-111111111111","seq":1,"status":"PLACED"}`)
	records := []dump.Record{{Partition: 0, Offset: 0, TimestampMS: at(1_020).UnixMilli(), TimestampType: logAppendTime, Value: value}}
	for _, err := range []error{
		raw.WriteJSON(dir, raw.TruthFile, truth),
		raw.WriteJSON(dir, raw.TimelineFile, raw.Timeline{CycleID: "X_P1_r01"}),
		raw.WriteJSON(dir, raw.LoadFile, raw.Load{}),
		raw.WriteBytes(dir, raw.MarkersFile, []byte(`{"msg":"SENT_CALLED","event_id":"e","order_id":"11111111-1111-1111-1111-111111111111","seq":1}`+"\n")),
		writeDump(raw.DumpPath(session, "X_P1_r01"), records),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := FromRaw(session, "X_P1_r01")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := Check(loaded.Input)
	if got.EventsTotal != 1 || got.Lost != 0 || got.WindowMaxMS != 20 || len(loaded.Input.Sent) != 1 ||
		loaded.Input.Kafka[0].EventID != "22222222-2222-2222-2222-222222222222" {
		t.Fatalf("got %+v from %+v", got, loaded.Input)
	}
	// A CreateTime record is refused, as ReadKafka refuses it.
	records[0].TimestampType = 0
	if err := writeDump(raw.DumpPath(session, "X_P1_r01"), records); err != nil {
		t.Fatal(err)
	}
	if _, err := FromRaw(session, "X_P1_r01"); err == nil {
		t.Fatal("FromRaw accepted a CreateTime record")
	}
}

func writeDump(path string, records []dump.Record) error {
	return dump.WriteForTest(filepath.Clean(path), records)
}

func TestMarkerLines(t *testing.T) {
	logs := []byte("{\"msg\":\"started\"}\nnot json\n{\"msg\":\"SENT_CALLED\",\"order_id\":\"a\",\"seq\":1}\n{\"msg\":\"CRASH_POINT_HIT\",\"point\":\"x\"}\n")
	got, err := MarkerLines(logs)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\"msg\":\"SENT_CALLED\",\"order_id\":\"a\",\"seq\":1}\n{\"msg\":\"CRASH_POINT_HIT\",\"point\":\"x\"}\n"
	if string(got) != want {
		t.Fatalf("got %q", got)
	}
}
