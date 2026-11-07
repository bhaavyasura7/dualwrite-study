// Package checker compares what Postgres committed with what Kafka holds (SPEC 11).
package checker

import (
	"cmp"
	"maps"
	"slices"
	"time"

	"dualwrite-study/internal/obs"
)

// Pair identifies one committed version of an order.
type Pair struct {
	OrderID string `json:"order"`
	Seq     int    `json:"seq"`
}

// Version is one row of orders_history.
type Version struct {
	Pair
	CommitTime time.Time `json:"commit_time"`
}

// Record is one Kafka record.
type Record struct {
	Pair
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

// Input is everything one cycle produced.
type Input struct {
	DB              []Version      `json:"db"`
	Kafka           []Record       `json:"kafka"`
	Sent            []Pair         `json:"sent"`        // SENT_CALLED markers
	Acked           []Pair         `json:"acked"`       // pairs with a SEND_ACKED marker
	Hooked          []Pair         `json:"hooked"`      // events named by CRASH_POINT_HIT
	SendFailed      int            `json:"send_failed"` // SEND_FAILED markers
	InflightAtCrash int            `json:"inflight_at_crash"`
	OldestPendingMS float64        `json:"oldest_pending_ms"`
	BatchRows       []int          `json:"batch_rows"`  // rows per relay batch, from RELAY_BATCH
	Payments        map[string]int `json:"payments"`    // rows per consumer strategy; nil outside group G
	PaidOrders      int            `json:"paid_orders"` // orders whose final status is PAID
}

// Result holds the metrics of Section 11.2. Names match the results CSV.
type Result struct {
	OrdersTotal int `json:"orders_total"`
	EventsTotal int `json:"events_total"`
	Lost        int `json:"lost"`
	Ghost       int `json:"ghost"`
	Duplicates  int `json:"duplicates"`
	OutOfOrder  int `json:"out_of_order"`
	// StaleRedelivery is a subset of Duplicates, not additive with it.
	StaleRedelivery int `json:"stale_redelivery"`
	LostAfterSend   int `json:"lost_after_send"`
	// LostAfterAck counts events the broker acknowledged and then lost.
	LostAfterAck   int `json:"lost_after_ack"`
	HookedLost     int `json:"hooked_lost"`
	HookedDup      int `json:"hooked_dup"`
	HookedGhost    int `json:"hooked_ghost"` // for assert_scope: hooked; not a CSV column
	CollateralLost int `json:"collateral_lost"`
	CollateralDup  int `json:"collateral_dup"`
	SendFailed     int `json:"send_failed"`
	// In-flight state at the crash, so collateral loss can be reported against it.
	InflightAtCrash int     `json:"inflight_at_crash"`
	OldestPendingMS float64 `json:"oldest_pending_ms"`
	// Actual relay batch sizes, which can be far below RELAY_BATCH_SIZE.
	RelayBatches   int            `json:"relay_batches"`
	BatchRowsMean  float64        `json:"batch_rows_mean"`
	BatchRowsMax   int            `json:"batch_rows_max"`
	BusinessDamage map[string]int `json:"business_damage,omitempty"`
	WindowP50MS    float64        `json:"window_p50_ms"`
	WindowP99MS    float64        `json:"window_p99_ms"`
	WindowMaxMS    float64        `json:"window_max_ms"`
}

// Damage lists the exact pairs behind each metric, for root-cause analysis.
type Damage struct {
	Lost            []Pair      `json:"lost"`
	LostDetail      []LostEvent `json:"lost_detail"`
	Ghost           []Pair      `json:"ghost"`
	Duplicates      []Pair      `json:"duplicates"` // one entry per extra copy
	OutOfOrder      []Pair      `json:"out_of_order"`
	StaleRedelivery []Pair      `json:"stale_redelivery"`
	LostAfterSend   []Pair      `json:"lost_after_send"`
	LostAfterAck    []Pair      `json:"lost_after_ack"`
}

// LostEvent explains one lost pair.
type LostEvent struct {
	Pair
	CommitTime   time.Time `json:"commit_time"`
	FinalVersion int       `json:"final_version"` // the order's last committed version
	Intermediate bool      `json:"intermediate"`  // a later version of the order exists
}

// Check computes the cycle's metrics. Input order does not matter.
func Check(in Input) (Result, Damage) {
	var damage Damage
	result := Result{
		EventsTotal:     len(in.DB),
		SendFailed:      in.SendFailed,
		InflightAtCrash: in.InflightAtCrash,
		OldestPendingMS: in.OldestPendingMS,
	}

	result.RelayBatches = len(in.BatchRows)
	if len(in.BatchRows) > 0 {
		total := 0
		for _, rows := range in.BatchRows {
			total += rows
			result.BatchRowsMax = max(result.BatchRowsMax, rows)
		}
		result.BatchRowsMean = float64(total) / float64(len(in.BatchRows))
	}

	committed := make(map[Pair]time.Time, len(in.DB))
	finalVersion := make(map[string]int)
	for _, v := range in.DB {
		committed[v.Pair] = v.CommitTime
		finalVersion[v.OrderID] = max(finalVersion[v.OrderID], v.Seq)
	}
	result.OrdersTotal = len(finalVersion)

	copies := make(map[Pair]int, len(in.Kafka))
	first := make(map[Pair]time.Time, len(in.Kafka))
	for _, r := range in.Kafka {
		copies[r.Pair]++
		if t, seen := first[r.Pair]; !seen || r.Timestamp.Before(t) {
			first[r.Pair] = r.Timestamp
		}
	}
	sent := toSet(in.Sent)
	acked := toSet(in.Acked)
	hooked := toSet(in.Hooked)

	for _, pair := range sortedKeys(committed) {
		if copies[pair] > 0 {
			continue
		}
		damage.Lost = append(damage.Lost, pair)
		final := finalVersion[pair.OrderID]
		damage.LostDetail = append(damage.LostDetail, LostEvent{pair, committed[pair], final, pair.Seq < final})
		if _, ok := hooked[pair]; ok {
			result.HookedLost++
		} else {
			result.CollateralLost++
		}
		if _, ok := sent[pair]; ok {
			damage.LostAfterSend = append(damage.LostAfterSend, pair)
		}
		if _, ok := acked[pair]; ok {
			damage.LostAfterAck = append(damage.LostAfterAck, pair)
		}
	}
	for _, pair := range sortedKeys(copies) {
		if _, ok := committed[pair]; !ok {
			damage.Ghost = append(damage.Ghost, pair)
			if _, ok := hooked[pair]; ok {
				result.HookedGhost++
			}
		}
		for range copies[pair] - 1 {
			damage.Duplicates = append(damage.Duplicates, pair)
			if _, ok := hooked[pair]; ok {
				result.HookedDup++
			} else {
				result.CollateralDup++
			}
		}
	}
	damage.OutOfOrder, damage.StaleRedelivery = ordering(in.Kafka)

	result.Lost = len(damage.Lost)
	result.Ghost = len(damage.Ghost)
	result.Duplicates = len(damage.Duplicates)
	result.OutOfOrder = len(damage.OutOfOrder)
	result.StaleRedelivery = len(damage.StaleRedelivery)
	result.LostAfterSend = len(damage.LostAfterSend)
	result.LostAfterAck = len(damage.LostAfterAck)

	windows := make([]float64, 0, len(first))
	for pair, seen := range first {
		if commit, ok := committed[pair]; ok {
			windows = append(windows, float64(seen.Sub(commit))/float64(time.Millisecond))
		}
	}
	slices.Sort(windows)
	result.WindowP50MS = obs.Percentile(windows, 50)
	result.WindowP99MS = obs.Percentile(windows, 99)
	result.WindowMaxMS = obs.Percentile(windows, 100)

	if in.Payments != nil {
		result.BusinessDamage = make(map[string]int, len(in.Payments))
		for strategy, rows := range in.Payments {
			result.BusinessDamage[strategy] = max(rows-in.PaidOrders, in.PaidOrders-rows)
		}
	}
	return result, damage
}

// ordering walks every order in log order. A first delivery below the highest
// seq seen is out of order. A repeat delivery below it is a stale redelivery: it
// is already counted as a duplicate, but can regress a last-write-wins consumer.
// Pairs stand in for event IDs because P5 has none; in P1–P4 they map one to one.
func ordering(records []Record) (outOfOrder, stale []Pair) {
	sorted := slices.Clone(records)
	slices.SortFunc(sorted, func(a, b Record) int {
		return cmp.Or(cmp.Compare(a.Partition, b.Partition), cmp.Compare(a.Offset, b.Offset))
	})
	delivered := make(map[Pair]struct{}, len(sorted))
	highest := make(map[string]int)
	for _, r := range sorted {
		_, repeat := delivered[r.Pair]
		delivered[r.Pair] = struct{}{}
		if r.Seq < highest[r.OrderID] {
			if repeat {
				stale = append(stale, r.Pair)
			} else {
				outOfOrder = append(outOfOrder, r.Pair)
			}
		}
		highest[r.OrderID] = max(highest[r.OrderID], r.Seq)
	}
	return outOfOrder, stale
}

// RecoveryS is the time from faultEnd until every version committed by then had
// reached Kafka. It is computed afterwards from broker timestamps, which gives the
// same answer as sampling during the drain. ok is false if any is still lost.
func RecoveryS(in Input, faultEnd time.Time) (seconds float64, ok bool) {
	first := make(map[Pair]time.Time, len(in.Kafka))
	for _, r := range in.Kafka {
		if t, seen := first[r.Pair]; !seen || r.Timestamp.Before(t) {
			first[r.Pair] = r.Timestamp
		}
	}
	for _, v := range in.DB {
		if v.CommitTime.After(faultEnd) {
			continue
		}
		delivered, found := first[v.Pair]
		if !found {
			return 0, false
		}
		seconds = max(seconds, delivered.Sub(faultEnd).Seconds())
	}
	return seconds, true
}

func toSet(pairs []Pair) map[Pair]struct{} {
	set := make(map[Pair]struct{}, len(pairs))
	for _, p := range pairs {
		set[p] = struct{}{}
	}
	return set
}

func sortedKeys[V any](m map[Pair]V) []Pair {
	return slices.SortedFunc(maps.Keys(m), func(a, b Pair) int {
		return cmp.Or(cmp.Compare(a.OrderID, b.OrderID), cmp.Compare(a.Seq, b.Seq))
	})
}
