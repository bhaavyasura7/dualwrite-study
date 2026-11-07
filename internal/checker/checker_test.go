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

func TestParseLogs(t *testing.T) {
	logs := `{"msg":"orderservice started"}
{"msg":"SENT_CALLED","order_id":"a","seq":1}
goroutine 1 [running]:
{"msg":"SEND_FAILED","event_id":"e","error":"timeout"}
{"msg":"SEND_FAILED","event_id":"f","error":"timeout"}
{"msg":"CRASH_POINT_HIT","order_id":"b","seq":2,"point":"after_db_commit"}
`
	got, err := ParseLogs(strings.NewReader(logs))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Sent, []Pair{{"a", 1}}) || !slices.Equal(got.Hooked, []Pair{{"b", 2}}) || got.SendFailed != 2 {
		t.Fatalf("got %+v", got)
	}
}
