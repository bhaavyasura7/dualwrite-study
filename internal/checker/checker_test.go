package checker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestGolden(t *testing.T) {
	files, err := filepath.Glob("testdata/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("fixtures: %v, %v", files, err)
	}
	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			var fixture struct {
				Comment string `json:"comment"`
				Input   Input  `json:"input"`
				Want    Result `json:"want"`
			}
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&fixture); err != nil {
				t.Fatal(err)
			}
			got, _ := Check(fixture.Input)
			if !reflect.DeepEqual(got, fixture.Want) {
				t.Errorf("\n got %+v\nwant %+v", got, fixture.Want)
			}
		})
	}
}

// TestGoldenLarge plants 50 problems among 100,000 events. Generated rather than
// stored, because the fixture would be many megabytes.
func TestGoldenLarge(t *testing.T) {
	const orders = 50_000
	const (
		lost   = 15
		dups   = 15
		swaps  = 10
		ghosts = 10
	)
	rng := rand.New(rand.NewPCG(42, 42))
	picks := rng.Perm(orders)[:lost+dups+swaps]
	role := make(map[int]string, len(picks))
	for i, order := range picks {
		switch {
		case i < lost:
			role[order] = "lost"
		case i < lost+dups:
			role[order] = "dup"
		default:
			role[order] = "swap"
		}
	}

	var in Input
	var want Damage
	offsets := [3]int64{}
	emit := func(p Pair, partition int32) {
		in.Kafka = append(in.Kafka, Record{Pair: p, Partition: partition, Offset: offsets[partition]})
		offsets[partition]++
	}
	for order := range orders {
		id := fmt.Sprintf("order-%06d", order)
		one, two := Pair{id, 1}, Pair{id, 2}
		in.DB = append(in.DB, Version{Pair: one}, Version{Pair: two})
		partition := int32(order % 3)
		switch role[order] {
		case "lost":
			emit(one, partition)
			want.Lost = append(want.Lost, two)
		case "dup":
			emit(one, partition)
			emit(two, partition)
			emit(two, partition)
			want.Duplicates = append(want.Duplicates, two)
		case "swap":
			emit(two, partition)
			emit(one, partition)
			want.OutOfOrder = append(want.OutOfOrder, one)
		default:
			emit(one, partition)
			emit(two, partition)
		}
	}
	for i := range ghosts {
		ghost := Pair{fmt.Sprintf("ghost-%02d", i), 1}
		emit(ghost, int32(i%3))
		want.Ghost = append(want.Ghost, ghost)
	}
	// The checker must rely on offsets, not on the order records were read in.
	rng.Shuffle(len(in.Kafka), func(i, j int) { in.Kafka[i], in.Kafka[j] = in.Kafka[j], in.Kafka[i] })

	result, damage := Check(in)
	if result.EventsTotal != 100_000 {
		t.Fatalf("events = %d", result.EventsTotal)
	}
	for name, pairs := range map[string][2][]Pair{
		"lost":       {damage.Lost, want.Lost},
		"ghost":      {damage.Ghost, want.Ghost},
		"duplicates": {damage.Duplicates, want.Duplicates},
		"disorder":   {damage.OutOfOrder, want.OutOfOrder},
	} {
		got, expected := sorted(pairs[0]), sorted(pairs[1])
		if !slices.Equal(got, expected) {
			t.Errorf("%s: got %d pairs, want %d", name, len(got), len(expected))
		}
	}
	if total := result.Lost + result.Duplicates + result.OutOfOrder + result.Ghost; total != 50 {
		t.Errorf("found %d problems, want 50", total)
	}
}

func sorted(pairs []Pair) []Pair {
	out := slices.Clone(pairs)
	slices.SortFunc(out, func(a, b Pair) int {
		return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
	})
	return out
}

func TestParseLogsInflightAtCrash(t *testing.T) {
	logs := `{"time":"2026-11-20T10:00:00.000Z","msg":"SENT_CALLED","event_id":"a","order_id":"o1","seq":1}
{"time":"2026-11-20T10:00:00.040Z","msg":"SENT_CALLED","event_id":"b","order_id":"o2","seq":1}
{"time":"2026-11-20T10:00:00.050Z","msg":"SENT_CALLED","event_id":"c","order_id":"o3","seq":1}
{"time":"2026-11-20T10:00:00.060Z","msg":"SEND_ACKED","event_id":"a"}
{"time":"2026-11-20T10:00:00.100Z","msg":"CRASH_POINT_HIT","order_id":"o4","seq":1,"point":"after_db_commit"}
{"time":"2026-11-20T10:00:09.000Z","msg":"SENT_CALLED","event_id":"z","order_id":"o5","seq":1}
`
	got, err := ParseLogs(strings.NewReader(logs))
	if err != nil {
		t.Fatal(err)
	}
	if got.InflightAtCrash != 2 || got.OldestPendingMS != 60 {
		t.Fatalf("inflight = %d, oldest = %v ms", got.InflightAtCrash, got.OldestPendingMS)
	}
}

func TestLostDetailMarksIntermediateVersions(t *testing.T) {
	_, damage := Check(Input{DB: []Version{{Pair: Pair{"a", 1}}, {Pair: Pair{"a", 2}}, {Pair: Pair{"b", 1}}}})
	if len(damage.LostDetail) != 3 {
		t.Fatalf("detail = %+v", damage.LostDetail)
	}
	for _, lost := range damage.LostDetail {
		if want := lost.OrderID == "a" && lost.Seq == 1; lost.Intermediate != want {
			t.Errorf("%+v: intermediate = %v", lost.Pair, lost.Intermediate)
		}
	}
}

func TestLostAfterAck(t *testing.T) {
	logs := `{"msg":"SENT_CALLED","event_id":"e1","order_id":"a","seq":1}
{"msg":"SENT_CALLED","event_id":"e2","order_id":"b","seq":1}
{"msg":"SEND_ACKED","event_id":"e1"}
{"msg":"SEND_FAILED","event_id":"e2"}
`
	evidence, err := ParseLogs(strings.NewReader(logs))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := Check(Input{
		DB:    []Version{{Pair: Pair{"a", 1}}, {Pair: Pair{"b", 1}}},
		Sent:  evidence.Sent,
		Acked: evidence.Acked,
	})
	if got.Lost != 2 || got.LostAfterSend != 2 || got.LostAfterAck != 1 {
		t.Fatalf("got %+v", got)
	}
}

func TestLateDeliveries(t *testing.T) {
	moment := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	in := Input{
		DB: []Version{
			{Pair{"before-late", 1}, moment.Add(-time.Second)},
			{Pair{"before-early", 1}, moment.Add(-time.Second)},
			{Pair{"after", 1}, moment.Add(time.Second)},
		},
		Kafka: []Record{
			{Pair: Pair{"before-late", 1}, Timestamp: moment.Add(time.Second)},
			{Pair: Pair{"before-early", 1}, Timestamp: moment.Add(-time.Millisecond)},
			{Pair: Pair{"after", 1}, Timestamp: moment.Add(2 * time.Second)},
		},
	}
	if got := LateDeliveries(in, moment); got != 1 {
		t.Fatalf("late = %d, want 1", got)
	}
}

func TestRecoveryS(t *testing.T) {
	end := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	in := Input{
		DB: []Version{
			{Pair{"a", 1}, end.Add(-time.Second)},
			{Pair{"b", 1}, end.Add(-time.Second)},
			{Pair{"late", 1}, end.Add(time.Minute)},
		},
		Kafka: []Record{
			{Pair: Pair{"a", 1}, Timestamp: end.Add(-500 * time.Millisecond)},
			{Pair: Pair{"b", 1}, Timestamp: end.Add(3 * time.Second)},
		},
	}
	if got, ok := RecoveryS(in, end); !ok || got != 3 {
		t.Fatalf("got %v, %v", got, ok)
	}
	in.Kafka = in.Kafka[:1]
	if _, ok := RecoveryS(in, end); ok {
		t.Fatal("reported recovery with a version still lost")
	}
}

func TestRelayBatchStats(t *testing.T) {
	logs := `{"msg":"RELAY_BATCH","n":10,"phase":"read"}
{"msg":"RELAY_BATCH","n":10,"phase":"published"}
{"msg":"RELAY_BATCH","n":10,"phase":"marked"}
{"msg":"RELAY_BATCH","n":30,"phase":"read"}
`
	evidence, err := ParseLogs(strings.NewReader(logs))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := Check(Input{BatchRows: evidence.BatchRows})
	if got.RelayBatches != 2 || got.BatchRowsMean != 20 || got.BatchRowsMax != 30 {
		t.Fatalf("got %+v", got)
	}
}

func TestParseLogs(t *testing.T) {
	logs := `{"msg":"orderservice started"}
{"msg":"SENT_CALLED","order_id":"a","seq":1}
goroutine 1 [running]:
{"msg":"SEND_FAILED","event_id":"e","error":"timeout"}
{"msg":"SEND_FAILED","event_id":"f","error":"timeout"}
{"msg":"CRASH_POINT_HIT","order_id":"b","seq":2,"point":"after_db_commit"}
{"msg":"CRASH_POINT_HIT","batch_id":"x","hooked":[{"order_id":"c","seq":1},{"order_id":"d","seq":3}],"point":"after_publish_before_mark"}
`
	got, err := ParseLogs(strings.NewReader(logs))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Sent, []Pair{{"a", 1}}) || !slices.Equal(got.Hooked, []Pair{{"b", 2}, {"c", 1}, {"d", 3}}) || got.SendFailed != 2 {
		t.Fatalf("got %+v", got)
	}
}
