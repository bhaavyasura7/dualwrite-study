package main

import (
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
