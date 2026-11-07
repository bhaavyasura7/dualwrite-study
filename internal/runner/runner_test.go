package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dualwrite-study/internal/checker"
)

const cataloguePath = "../../experiments/vectors.yaml"

func realCycles(t *testing.T) []Cycle {
	t.Helper()
	catalogue, err := LoadCatalogue(cataloguePath)
	if err != nil {
		t.Fatal(err)
	}
	cycles, err := catalogue.Expand()
	if err != nil {
		t.Fatal(err)
	}
	return cycles
}

func find(t *testing.T, cycles []Cycle, id string) Cycle {
	t.Helper()
	for _, c := range cycles {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("no cycle %s", id)
	return Cycle{}
}

func TestRealCatalogueExpands(t *testing.T) {
	cycles := realCycles(t)

	a1 := find(t, cycles, "A-1_P1_r30")
	if a1.Crash.Point != "after_db_commit" || a1.Env["KAFKA_LINGER_MS"] != "5" || a1.Load.RatePerS != 100 {
		t.Errorf("A-1 = %+v", a1)
	}
	// The sweep key sets the env, and YAML 1.1 "off" must stay a string.
	if got := find(t, cycles, "E-3_P4_c1_r01").Env["CONNECT_EOS"]; got != "off" {
		t.Errorf("CONNECT_EOS = %q", got)
	}
	// Sweep values reach the right place.
	if got := find(t, cycles, "A-4_P3_c4_r01").Env["RELAY_BATCH_SIZE"]; got != "500" {
		t.Errorf("batch = %s", got)
	}
	if got := find(t, cycles, "C-1_P1_c3_r01").Faults[0].DurationS.Lo; got != 300 {
		t.Errorf("C-1 duration = %v", got)
	}
	if got := find(t, cycles, "F-1_P1_c3_r01").Faults[0].Params["latency_ms"]; got != 1000 {
		t.Errorf("F-1 latency = %v", got)
	}
	if got := find(t, cycles, "P-1_P3_c3_r01").Load.RatePerS; got != 1000 {
		t.Errorf("P-1 rate = %d", got)
	}
	if got := find(t, cycles, "H-3_P3_c2_r01").Topology.Relays; got != 4 {
		t.Errorf("H-3 relays = %d", got)
	}
	// Cells: only acks differs in C-4.
	one, all := find(t, cycles, "C-4_P1_c1_r01"), find(t, cycles, "C-4_P1_c2_r01")
	if one.Env["KAFKA_ACKS"] != "1" || all.Env["KAFKA_ACKS"] != "all" || one.Env["KAFKA_IDEMPOTENT"] != "false" || all.Env["KAFKA_IDEMPOTENT"] != "false" {
		t.Errorf("C-4 cells = %v / %v", one.Env, all.Env)
	}
	// by_pattern replaces the fault list.
	if got := find(t, cycles, "C-3_P3_r01").Faults[1].Target; got != "relay" {
		t.Errorf("C-3 target = %s", got)
	}
	if find(t, cycles, "G-3_P3_r01").Crash == nil || find(t, cycles, "G-3_P4_r01").Crash != nil {
		t.Error("G-3 by_pattern crash wrong")
	}
	// Sweeping one cell must not leak into another.
	if got := find(t, cycles, "F-1_P1_c1_r01").Faults[0].Params["latency_ms"]; got != 200 {
		t.Errorf("F-1 cell 1 latency = %v", got)
	}
}

func TestUnknownKeysAreRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vectors.yaml")
	raw, err := os.ReadFile(cataloguePath)
	if err != nil {
		t.Fatal(err)
	}
	broken := strings.Replace(string(raw), "    priority: must\n", "    priorty: must\n", 1)
	if err := os.WriteFile(path, []byte(broken), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCatalogue(path); err == nil {
		t.Fatal("typo accepted")
	}
}

func TestShardsSplitByVectorWithoutOverlap(t *testing.T) {
	cycles := realCycles(t)
	const n = 4
	owner := make(map[string]int)
	seen := 0
	for i := 1; i <= n; i++ {
		shard := Shard{Index: i, Count: n}
		for _, c := range cycles {
			if !shard.Includes(c) {
				continue
			}
			seen++
			if other, ok := owner[c.VectorID]; ok && other != i {
				t.Fatalf("vector %s split across shards %d and %d", c.VectorID, other, i)
			}
			owner[c.VectorID] = i
			if c.Group == "perf" {
				t.Fatalf("perf cycle %s sharded", c.ID)
			}
		}
	}
	perf := 0
	for _, c := range cycles {
		if c.Group == "perf" {
			perf++
		}
	}
	if seen != len(cycles)-perf {
		t.Fatalf("shards cover %d of %d", seen, len(cycles)-perf)
	}
}

func TestParseShard(t *testing.T) {
	if s, err := ParseShard("2/4"); err != nil || s.Index != 2 || s.Count != 4 {
		t.Errorf("got %+v, %v", s, err)
	}
	for _, bad := range []string{"0/4", "5/4", "x", "1/0"} {
		if _, err := ParseShard(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

func TestAssertScope(t *testing.T) {
	result := checker.Result{Lost: 5, CollateralLost: 5}
	cycle := Cycle{Assert: map[string]int{"lost": 0, "ghost": 0}}
	if assertionFailure(cycle, result) == "" {
		t.Error("collateral loss should fail an assert over all events")
	}
	cycle.AssertScope = "hooked"
	if got := assertionFailure(cycle, result); got != "" {
		t.Errorf("collateral loss failed a hooked assert: %s", got)
	}
	result.HookedGhost = 1
	if assertionFailure(cycle, result) == "" {
		t.Error("hooked ghost should fail")
	}
}

func row(id, commit string) Row {
	now := time.Now()
	return Row{CycleID: id, GitCommit: commit, VersionsJSON: "{}", StartedAt: now, FinishedAt: now}
}

func TestResultsResumeAndRefuseOtherCommit(t *testing.T) {
	if len(row("a", "c").record()) != len(columns) {
		t.Fatalf("record has %d fields, header has %d", len(row("a", "c").record()), len(columns))
	}
	path := filepath.Join(t.TempDir(), "results.csv")
	results, err := OpenResults(path, "abc")
	if err != nil {
		t.Fatal(err)
	}
	if err := results.Append(row("A-1_P1_r01", "abc")); err != nil {
		t.Fatal(err)
	}
	if err := results.Append(row("A-1_P1_r02", "abc")); err != nil {
		t.Fatal(err)
	}
	resumed, err := OpenResults(path, "abc")
	if err != nil {
		t.Fatal(err)
	}
	if !resumed.Done("A-1_P1_r02") || resumed.Done("A-1_P1_r03") {
		t.Fatal("resume state wrong")
	}
	if _, err := OpenResults(path, "other"); err == nil {
		t.Fatal("mixed commits accepted")
	}
}

func TestMerge(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, rows ...Row) string {
		path := filepath.Join(dir, name)
		results := &Results{path: path, done: map[string]struct{}{}}
		for _, r := range rows {
			if err := results.Append(r); err != nil {
				t.Fatal(err)
			}
		}
		return path
	}
	a := write("results.a.csv", row("x", "abc"))
	b := write("results.b.csv", row("y", "abc"))
	target := filepath.Join(dir, "results.csv")
	if n, err := Merge([]string{a, b}, target); err != nil || n != 2 {
		t.Fatalf("merged %d, %v", n, err)
	}
	if _, err := Merge([]string{a, write("results.c.csv", row("x", "abc"))}, target); err == nil {
		t.Error("duplicate cycle accepted")
	}
	if _, err := Merge([]string{a, write("results.d.csv", row("z", "other"))}, target); err == nil {
		t.Error("mixed commits accepted")
	}
}

func TestConnectorConfig(t *testing.T) {
	t.Chdir("../..")
	for pattern, want := range map[string]string{"P4": `"orders.A-1b_P4_r02"`, "P5": `"orders.A-1b_P5_r02"`} {
		raw, err := connectorConfig(Cycle{ID: "A-1b_" + pattern + "_r02", Pattern: pattern, Env: map[string]string{"KAFKA_ACKS": "all", "DEBEZIUM_POLL_INTERVAL_MS": "500", "DEBEZIUM_RETRY_WAIT_MS": "10000"}}, "abc-1", connector{})
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		var parsed map[string]any
		if err := json.Unmarshal(raw, &parsed); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(text, `_r02-abc-1"`) {
			t.Errorf("%s: name lacks the run id", pattern)
		}
		if strings.Contains(text, "{{") || !strings.Contains(text, want) || !strings.Contains(text, `"15433"`) {
			t.Errorf("%s: %s", pattern, text)
		}
		if !strings.Contains(text, `"slot_a_1b_`+strings.ToLower(pattern)+`_r02"`) {
			t.Errorf("%s: bad slot name", pattern)
		}
	}
}

func TestVerify(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.csv")
	results := &Results{path: path, done: map[string]struct{}{}}
	dirty := row("a", "abc-dirty")
	dirty.Status = "OK"
	for _, r := range []Row{dirty, dirty, row("b", "def")} {
		if err := results.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	got, err := Verify(path, []Cycle{{ID: "a"}, {ID: "b"}, {ID: "c"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Rows != 3 || len(got.Missing) != 1 || got.Missing[0] != "c" {
		t.Fatalf("got %+v", got)
	}
	want := []string{"rows from a dirty tree: abc-dirty", "2 commits in one file", "cycle appears twice: a"}
	if strings.Join(got.Problems, "|") != strings.Join(want, "|") {
		t.Fatalf("problems = %q", got.Problems)
	}
}
