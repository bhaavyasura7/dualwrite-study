package checker

import (
	"math"
	"slices"
	"time"

	"dualwrite-study/internal/obs"
	"dualwrite-study/internal/raw"
)

// Recovery is one recovery time (SPEC 11.2). Known is false for cycles without
// a fault, or when the time cannot be computed (no valid commit times); the CSV
// columns are then empty. When Recovered is false, Seconds is the censoring
// time, never a recovery time.
type Recovery struct {
	Known     bool
	Recovered bool
	Seconds   float64
}

// Recoveries holds the three recovery times and the window baseline.
type Recoveries struct {
	Service, Synchronization, Consistency Recovery
	WindowP99PreMS                        float64
	WindowP99PreKnown                     bool
}

// Timing is the part of the timeline the recovery times need.
type Timing struct {
	LoadStart, WarmupEnd, FirstFault, FaultEnd, LoadStop, ObservedUntil time.Time
}

const (
	healthyShare   = 0.95             // share of issued requests that must succeed
	healthyStretch = 5                // consecutive healthy seconds
	syncInterval   = 10 * time.Second // a full interval of prompt deliveries
	syncSlack      = time.Second      // added to the baseline p99
	baselineTail   = 5 * time.Second  // fallback baseline: the end of warm-up
)

// ComputeRecoveries computes service, synchronization and consistency recovery.
// resultInvalid is the window's validity from Check: commit times that were
// missing or impossible make the two commit-time-based times unknown.
func ComputeRecoveries(in Input, t Timing, requests []raw.Request, windowInvalid bool) Recoveries {
	var out Recoveries
	in = withoutMarkers(in)
	if t.FaultEnd.IsZero() {
		return out
	}
	out.Service = serviceRecovery(t, requests)
	if windowInvalid {
		return out
	}
	first := firstDeliveries(in.Kafka)
	out.Consistency = consistencyRecovery(in.DB, first, t)
	baseline, ok := baselineP99(in.DB, first, t)
	out.WindowP99PreMS, out.WindowP99PreKnown = baseline, ok
	if ok {
		out.Synchronization = synchronizationRecovery(in.DB, first, t, baseline)
	}
	return out
}

func firstDeliveries(records []Record) map[Pair]time.Time {
	first := make(map[Pair]time.Time, len(records))
	for _, r := range records {
		if f, seen := first[r.Pair]; !seen || r.Timestamp.Before(f) {
			first[r.Pair] = r.Timestamp
		}
	}
	return first
}

// consistencyRecovery: until every version committed before the fault ended
// has a first delivery. Censored at ObservedUntil.
func consistencyRecovery(db []Version, first map[Pair]time.Time, t Timing) Recovery {
	seconds := 0.0
	for _, v := range db {
		if v.CommitTime.After(t.FaultEnd) {
			continue
		}
		delivered, ok := first[v.Pair]
		if !ok || delivered.After(t.ObservedUntil) {
			return Recovery{Known: true, Seconds: t.ObservedUntil.Sub(t.FaultEnd).Seconds()}
		}
		seconds = max(seconds, delivered.Sub(t.FaultEnd).Seconds())
	}
	return Recovery{Known: true, Recovered: true, Seconds: seconds}
}

// serviceRecovery: the start of the first stretch of healthyStretch whole
// seconds, at or after the fault's end and before load stop, in each of which
// at least healthyShare of the issued requests succeeded. Seconds are counted
// from LoadStart by request start time. Censored at load stop.
func serviceRecovery(t Timing, requests []raw.Request) Recovery {
	censored := Recovery{Known: true, Seconds: t.LoadStop.Sub(t.FaultEnd).Seconds()}
	whole := int(t.LoadStop.Sub(t.LoadStart) / time.Second) // seconds wholly before load stop
	issued := make([]int, whole)
	succeeded := make([]int, whole)
	for _, r := range requests {
		i := int(r.StartMS / 1000)
		if i < 0 || i >= whole {
			continue
		}
		issued[i]++
		if r.OK {
			succeeded[i]++
		}
	}
	healthy := func(i int) bool {
		return issued[i] > 0 && float64(succeeded[i]) >= healthyShare*float64(issued[i])
	}
	startIndex := int(math.Ceil(t.FaultEnd.Sub(t.LoadStart).Seconds()))
	for i := max(startIndex, 0); i+healthyStretch <= whole; i++ {
		ok := true
		for j := i; j < i+healthyStretch; j++ {
			if !healthy(j) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		if i == startIndex {
			return Recovery{Known: true, Recovered: true} // never degraded after the fault
		}
		start := t.LoadStart.Add(time.Duration(i) * time.Second)
		return Recovery{Known: true, Recovered: true, Seconds: max(start.Sub(t.FaultEnd).Seconds(), 0)}
	}
	return censored
}

// baselineP99 is the window p99 of versions committed between the end of
// warm-up and the first fault, or, if none was, in the last baselineTail of
// warm-up (SPEC 11.2).
func baselineP99(db []Version, first map[Pair]time.Time, t Timing) (float64, bool) {
	firstFault := t.FirstFault
	if firstFault.IsZero() {
		firstFault = t.FaultEnd
	}
	collect := func(from, to time.Time) []float64 {
		var windows []float64
		for _, v := range db {
			if v.CommitTime.Before(from) || !v.CommitTime.Before(to) {
				continue
			}
			if delivered, ok := first[v.Pair]; ok {
				windows = append(windows, float64(delivered.Sub(v.CommitTime))/float64(time.Millisecond))
			}
		}
		slices.Sort(windows)
		return windows
	}
	windows := collect(t.WarmupEnd, firstFault)
	if len(windows) == 0 {
		windows = collect(t.WarmupEnd.Add(-baselineTail), t.WarmupEnd)
	}
	if len(windows) == 0 {
		return 0, false
	}
	return obs.Percentile(windows, 99), true
}

// synchronizationRecovery: the earliest whole second t after the fault's end
// such that every version committed from t to load stop was first delivered
// within baseline + syncSlack, with at least one full syncInterval of them.
// A lost version counts as late. Censored at load stop.
func synchronizationRecovery(db []Version, first map[Pair]time.Time, t Timing, baselineMS float64) Recovery {
	censored := Recovery{Known: true, Seconds: t.LoadStop.Sub(t.FaultEnd).Seconds()}
	limit := time.Duration(baselineMS*float64(time.Millisecond)) + syncSlack
	var lastLate time.Time // commit time of the latest late version after the fault
	for _, v := range db {
		if v.CommitTime.Before(t.FaultEnd) || !v.CommitTime.Before(t.LoadStop) {
			continue
		}
		delivered, ok := first[v.Pair]
		if !ok || delivered.Sub(v.CommitTime) > limit {
			if v.CommitTime.After(lastLate) {
				lastLate = v.CommitTime
			}
		}
	}
	start := t.FaultEnd
	if !lastLate.IsZero() {
		// The first whole second after the fault's end that is past the last late version.
		steps := int(lastLate.Sub(t.FaultEnd)/time.Second) + 1
		start = t.FaultEnd.Add(time.Duration(steps) * time.Second)
	}
	if t.LoadStop.Sub(start) < syncInterval {
		return censored
	}
	return Recovery{Known: true, Recovered: true, Seconds: start.Sub(t.FaultEnd).Seconds()}
}
