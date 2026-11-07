package crash

import (
	"testing"
	"time"
)

func arm(t *testing.T, after string) *int {
	t.Helper()
	t.Setenv("CRASH_AT", "here")
	t.Setenv("CRASH_AFTER_N", after)
	hits.Store(0)
	kills := 0
	original := kill
	kill = func() { kills++ }
	t.Cleanup(func() { kill = original })
	return &kills
}

func TestPointFiresOnlyOnNthHit(t *testing.T) {
	kills := arm(t, "3")

	Point("here")
	Point("here")
	if *kills != 0 {
		t.Fatalf("fired early: %d", *kills)
	}
	Point("here")
	if *kills != 1 {
		t.Fatalf("kills = %d, want 1", *kills)
	}
	Point("here")
	if *kills != 1 {
		t.Fatalf("fired again: %d", *kills)
	}
}

func TestOtherPointsDoNotCount(t *testing.T) {
	kills := arm(t, "1")

	Point("elsewhere")
	Point("")
	if *kills != 0 || hits.Load() != 0 {
		t.Fatalf("kills = %d, hits = %d", *kills, hits.Load())
	}
}

func TestDefaultAfterN(t *testing.T) {
	kills := arm(t, "")

	for range defaultAfterN - 1 {
		Point("here")
	}
	if *kills != 0 {
		t.Fatal("fired before the default N")
	}
	Point("here")
	if *kills != 1 {
		t.Fatalf("kills = %d, want 1", *kills)
	}
}

func TestInvalidAfterNPanics(t *testing.T) {
	arm(t, "0")
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	Point("here")
}

func TestStallFiresOnceOnNthHit(t *testing.T) {
	t.Setenv("STALL_AT", "here")
	t.Setenv("STALL_AFTER_N", "2")
	t.Setenv("STALL_MS", "1500")
	stallHits.Store(0)
	var slept []time.Duration
	original := sleep
	sleep = func(d time.Duration) { slept = append(slept, d) }
	t.Cleanup(func() { sleep = original })

	Stall("elsewhere")
	Stall("here")
	if len(slept) != 0 {
		t.Fatalf("stalled early: %v", slept)
	}
	Stall("here")
	Stall("here")
	if len(slept) != 1 || slept[0] != 1500*time.Millisecond {
		t.Fatalf("slept = %v", slept)
	}
}

func TestPointsWaitForArming(t *testing.T) {
	kills := arm(t, "1")
	waiting.Store(true)
	t.Cleanup(func() { waiting.Store(false) })

	Point("here")
	if *kills != 0 || hits.Load() != 0 {
		t.Fatalf("counted before arming: kills = %d, hits = %d", *kills, hits.Load())
	}
	waiting.Store(false)
	Point("here")
	if *kills != 1 {
		t.Fatalf("kills = %d, want 1", *kills)
	}
}
