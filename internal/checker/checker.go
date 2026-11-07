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
	DB         []Version      `json:"db"`
	Kafka      []Record       `json:"kafka"`
	Sent       []Pair         `json:"sent"`        // SENT_CALLED markers
	Hooked     []Pair         `json:"hooked"`      // events named by CRASH_POINT_HIT
	SendFailed int            `json:"send_failed"` // SEND_FAILED markers
	Payments   map[string]int `json:"payments"`    // rows per consumer strategy; nil outside group G
	PaidOrders int            `json:"paid_orders"` // orders whose final status is PAID
}

// Result holds the metrics of Section 11.2. Names match the results CSV.
type Result struct {
	OrdersTotal    int            `json:"orders_total"`
	EventsTotal    int            `json:"events_total"`
	Lost           int            `json:"lost"`
	Ghost          int            `json:"ghost"`
	Duplicates     int            `json:"duplicates"`
	OutOfOrder     int            `json:"out_of_order"`
	LostAfterSend  int            `json:"lost_after_send"`
	HookedLost     int            `json:"hooked_lost"`
	HookedDup      int            `json:"hooked_dup"`
	HookedGhost    int            `json:"hooked_ghost"` // for assert_scope: hooked; not a CSV column
	CollateralLost int            `json:"collateral_lost"`
	CollateralDup  int            `json:"collateral_dup"`
	SendFailed     int            `json:"send_failed"`
	BusinessDamage map[string]int `json:"business_damage,omitempty"`
	WindowP50MS    float64        `json:"window_p50_ms"`
	WindowP99MS    float64        `json:"window_p99_ms"`
	WindowMaxMS    float64        `json:"window_max_ms"`
}

// Damage lists the exact pairs behind each metric, for root-cause analysis.
type Damage struct {
	Lost          []Pair `json:"lost"`
	Ghost         []Pair `json:"ghost"`
	Duplicates    []Pair `json:"duplicates"` // one entry per extra copy
	OutOfOrder    []Pair `json:"out_of_order"`
	LostAfterSend []Pair `json:"lost_after_send"`
}

// Check computes the cycle's metrics. Input order does not matter.
func Check(in Input) (Result, Damage) {
	var damage Damage
	result := Result{EventsTotal: len(in.DB), SendFailed: in.SendFailed}

	committed := make(map[Pair]time.Time, len(in.DB))
	orders := make(map[string]struct{})
	for _, v := range in.DB {
		committed[v.Pair] = v.CommitTime
		orders[v.OrderID] = struct{}{}
	}
	result.OrdersTotal = len(orders)

	copies := make(map[Pair]int, len(in.Kafka))
	first := make(map[Pair]time.Time, len(in.Kafka))
	for _, r := range in.Kafka {
		copies[r.Pair]++
		if t, seen := first[r.Pair]; !seen || r.Timestamp.Before(t) {
			first[r.Pair] = r.Timestamp
		}
	}
	sent := toSet(in.Sent)
	hooked := toSet(in.Hooked)

	for _, pair := range sortedKeys(committed) {
		if copies[pair] > 0 {
			continue
		}
		damage.Lost = append(damage.Lost, pair)
		if _, ok := hooked[pair]; ok {
			result.HookedLost++
		} else {
			result.CollateralLost++
		}
		if _, ok := sent[pair]; ok {
			damage.LostAfterSend = append(damage.LostAfterSend, pair)
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
	damage.OutOfOrder = outOfOrder(in.Kafka)

	result.Lost = len(damage.Lost)
	result.Ghost = len(damage.Ghost)
	result.Duplicates = len(damage.Duplicates)
	result.OutOfOrder = len(damage.OutOfOrder)
	result.LostAfterSend = len(damage.LostAfterSend)

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

// outOfOrder reports each record whose seq is below one already seen for its
// order, walking every order in log order.
func outOfOrder(records []Record) []Pair {
	sorted := slices.Clone(records)
	slices.SortFunc(sorted, func(a, b Record) int {
		return cmp.Or(cmp.Compare(a.Partition, b.Partition), cmp.Compare(a.Offset, b.Offset))
	})
	var found []Pair
	highest := make(map[string]int)
	for _, r := range sorted {
		if r.Seq < highest[r.OrderID] {
			found = append(found, r.Pair)
		}
		highest[r.OrderID] = max(highest[r.OrderID], r.Seq)
	}
	return found
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
