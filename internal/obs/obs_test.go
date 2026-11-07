package obs

import "testing"

func TestPercentile(t *testing.T) {
	values := make([]float64, 100)
	for i := range values {
		values[i] = float64(i + 1)
	}
	for q, want := range map[float64]float64{50: 50, 99: 99, 100: 100, 0: 1} {
		if got := Percentile(values, q); got != want {
			t.Errorf("p%v = %v, want %v", q, got, want)
		}
	}
	if got := Percentile(nil, 50); got != 0 {
		t.Errorf("empty = %v", got)
	}
}
