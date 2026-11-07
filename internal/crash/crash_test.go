package crash

import "testing"

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
