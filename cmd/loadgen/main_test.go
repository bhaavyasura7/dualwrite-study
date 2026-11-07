package main

import (
	"math/rand/v2"
	"testing"
	"time"

	"dualwrite-study/internal/config"
)

func TestIntervalFollowsBurstWindow(t *testing.T) {
	cfg := config.LoadGen{Mode: "burst", Rate: 100, BurstRate: 1000, BurstAt: 10 * time.Second, BurstLength: 10 * time.Second}
	for elapsed, want := range map[time.Duration]time.Duration{
		9 * time.Second:  10 * time.Millisecond,
		10 * time.Second: time.Millisecond,
		19 * time.Second: time.Millisecond,
		20 * time.Second: 10 * time.Millisecond,
	} {
		if got := interval(cfg, elapsed); got != want {
			t.Errorf("at %v: interval = %v, want %v", elapsed, got, want)
		}
	}
	cfg.Mode = "constant"
	if got := interval(cfg, 15*time.Second); got != 10*time.Millisecond {
		t.Errorf("constant mode bursts: %v", got)
	}
}

func TestFollowUpMix(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 1))
	counts := map[string]int{}
	for range 100_000 {
		counts[followUp(rng.Float64())]++
	}
	for action, want := range map[string]int{"pay": 60_000, "cancel": 20_000, "": 20_000} {
		if got := counts[action]; got < want-1000 || got > want+1000 {
			t.Errorf("%q: %d, want about %d", action, got, want)
		}
	}
}
