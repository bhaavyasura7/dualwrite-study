package main

import (
	"maps"
	"path/filepath"
	"testing"

	"dualwrite-study/internal/runner"
)

func TestC01RunsFirstAndRestIsShuffled(t *testing.T) {
	catalogue, err := runner.LoadCatalogue("../../experiments/vectors.yaml")
	if err != nil {
		t.Fatal(err)
	}
	all, err := catalogue.Expand()
	if err != nil {
		t.Fatal(err)
	}
	cycles := selectCycles(all, nil, nil, 0, runner.Shard{}, false)
	shuffled := shuffleWithinGroups(cycles, 1)

	c01 := 0
	for _, c := range cycles {
		if c.VectorID == "C0-1" {
			c01++
		}
	}
	for i, c := range shuffled[:c01] {
		if c.VectorID != "C0-1" {
			t.Fatalf("position %d is %s, want C0-1", i+1, c.ID)
		}
	}
	for _, c := range shuffled[c01:] {
		if c.VectorID == "C0-1" {
			t.Fatalf("C0-1 cycle %s after another vector", c.ID)
		}
	}
	if shuffled[c01].VectorID != "C0-3" && shuffled[c01].VectorID != "C0-3r" {
		t.Errorf("controls must follow C0-1, got %s", shuffled[c01].ID)
	}
	moved := 0
	for i := range cycles {
		if cycles[i].ID != shuffled[i].ID {
			moved++
		}
	}
	if moved < len(cycles)/2 {
		t.Errorf("only %d of %d cycles moved; order is not shuffled", moved, len(cycles))
	}
	again := shuffleWithinGroups(cycles, 1)
	for i := range shuffled {
		if shuffled[i].ID != again[i].ID {
			t.Fatal("same seed gave a different order; resume would break")
		}
	}
	last := shuffled[len(shuffled)-1]
	if !last.RequiresRebuild {
		t.Errorf("failover cycles must run last, got %s", last.ID)
	}
}

func loadAll(t *testing.T) []runner.Cycle {
	t.Helper()
	catalogue, err := runner.LoadCatalogue("../../experiments/vectors.yaml")
	if err != nil {
		t.Fatal(err)
	}
	all, err := catalogue.Expand()
	if err != nil {
		t.Fatal(err)
	}
	return all
}

// TestCheckRunCycles pins the check run of SPEC 14.7 to its list.
func TestCheckRunCycles(t *testing.T) {
	got := map[string]int{}
	for _, c := range checkRunCycles(loadAll(t)) {
		got[c.VectorID+" "+c.Pattern]++
		if c.VectorID == "P-1" && c.Load.RatePerS != 1000 {
			t.Errorf("%s: P-1 at %d/s, want 1000", c.ID, c.Load.RatePerS)
		}
	}
	want := map[string]int{
		"C0-1 P4": 1, "C0-3 P1": 1, "C0-3 P2": 1, "C0-3 P3": 1, "C0-3 P4": 1, "C0-3 P5": 1,
		"C-1 P1": 3, "C-1 P4": 3, "A-1 P1": 3, "A-3 P2": 1, "E-2 P4": 3,
		"D-1 P1": 2, "D-1 P4": 2, "D-2 P4": 1, "B-1 P3": 1, "G-1 P3": 1, "G-2 P3": 1,
		"P-1 P3": 1, "P-1 P4": 1,
	}
	if !maps.Equal(got, want) {
		t.Fatalf("check run:\n got %v\nwant %v", got, want)
	}
}

func TestSessionsPartitionThePaperRun(t *testing.T) {
	all := loadAll(t)
	counts := map[string]int{}
	for _, c := range all {
		n := 0
		for name, in := range sessions {
			if in(c) {
				counts[name]++
				n++
			}
		}
		if n > 1 {
			t.Fatalf("%s is in %d sessions", c.ID, n)
		}
	}
	// SPEC 1.1 and 14.7: 2,125 must, 1,390 should, 75 perf.
	if counts["must"] != 2125 || counts["should"] != 1390 || counts["perf"] != 75 {
		t.Fatalf("sessions: %v", counts)
	}
}

func TestParseOptionsSession(t *testing.T) {
	if _, err := parseOptions([]string{"-session", "must"}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]string{
		{"-session", "optional"},
		{"-session", "must", "-vectors", "A-1"},
		{"-session", "must", "-allow-dirty"},
		{"-session", "must", "-cycles", "x"},
		{"stray"},
	} {
		if _, err := parseOptions(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	o, err := parseOptions([]string{"-check-run"})
	if err != nil || o.trial != "check" {
		t.Fatalf("check-run: %+v, %v", o, err)
	}
	if dir := runDir(o, "abc"); dir != filepath.Join("results", "trials", "abc-check") {
		t.Errorf("check-run dir %s", dir)
	}
	if dir := runDir(options{session: "should"}, "abc"); dir != filepath.Join("results", "runs", "abc", "should") {
		t.Errorf("session dir %s", dir)
	}
}

func TestManifestRefusesOtherSelection(t *testing.T) {
	dir := t.TempDir()
	catalogue := "../../experiments/vectors.yaml"
	if err := writeManifest(dir, "abc", 1, catalogue, []string{"-session", "must"}, "h", "must"); err != nil {
		t.Fatal(err)
	}
	if err := writeManifest(dir, "abc", 1, catalogue, []string{"-session", "must"}, "h", "must"); err != nil {
		t.Fatalf("identical resume refused: %v", err)
	}
	for name, try := range map[string]func() error{
		"commit": func() error {
			return writeManifest(dir, "def", 1, catalogue, []string{"-session", "must"}, "h", "must")
		},
		"seed": func() error {
			return writeManifest(dir, "abc", 2, catalogue, []string{"-session", "must"}, "h", "must")
		},
		"session": func() error {
			return writeManifest(dir, "abc", 1, catalogue, []string{"-session", "must"}, "h", "should")
		},
		"arguments": func() error {
			return writeManifest(dir, "abc", 1, catalogue, []string{"-session", "must", "-seed", "1"}, "h", "must")
		},
	} {
		if try() == nil {
			t.Errorf("resume with a different %s accepted", name)
		}
	}
}

// TestSelectionIsReproducible: verify rebuilds a session's order from its
// manifest arguments alone, so the same arguments must give the same order.
func TestSelectionIsReproducible(t *testing.T) {
	o, err := parseOptions([]string{"-session", "perf", "-catalogue", "../../experiments/vectors.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	a, err := selection(o)
	if err != nil {
		t.Fatal(err)
	}
	b, err := selection(o)
	if err != nil {
		t.Fatal(err)
	}
	if orderText(a) != orderText(b) || len(a) != 75 {
		t.Fatalf("order not reproducible or wrong size (%d)", len(a))
	}
}
