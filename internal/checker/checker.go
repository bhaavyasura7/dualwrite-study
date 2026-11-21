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
	Status     string    `json:"status,omitempty"`
	CommitTime time.Time `json:"commit_time"`
	// CommitMissing is set when Postgres returned no commit time (SPEC 5.1).
	CommitMissing bool `json:"commit_missing,omitempty"`
}

// Record is one Kafka record.
type Record struct {
	Pair
	EventID   string    `json:"event_id,omitempty"` // empty for P5
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

// Order is an order's final state in the truth, for business damage.
type Order struct {
	OrderID     string `json:"order"`
	Status      string `json:"status"`
	AmountCents int64  `json:"amount_cents"`
}

// Payment is one row of a consumer payment table.
type Payment struct {
	PaymentID   int64  `json:"payment_id"`
	OrderID     string `json:"order"`
	EventKey    string `json:"event_key"`
	AmountCents int64  `json:"amount_cents"`
}

// windowTolerance is how far a first delivery may precede its commit time
// before the pair counts as impossible (SPEC 5.1): broker and Postgres share
// the host's clock, and broker timestamps have millisecond resolution.
const windowTolerance = 5 * time.Millisecond

// Input is everything one cycle produced.
type Input struct {
	DB              []Version `json:"db"`
	Kafka           []Record  `json:"kafka"`
	Sent            []Pair    `json:"sent"`        // SENT_CALLED markers
	Acked           []Pair    `json:"acked"`       // pairs with a SEND_ACKED marker
	Hooked          []Pair    `json:"hooked"`      // events named by CRASH_POINT_HIT
	SendFailed      int       `json:"send_failed"` // SEND_FAILED markers
	InflightAtCrash int       `json:"inflight_at_crash"`
	OldestPendingMS float64   `json:"oldest_pending_ms"`
	BatchRows       []int     `json:"batch_rows"` // rows per relay batch, from RELAY_BATCH
	// Business damage inputs (group G); Payments is nil outside group G.
	Orders   []Order              `json:"orders,omitempty"`
	Payments map[string][]Payment `json:"payments,omitempty"` // by strategy
	// MarkerIDs are the drain markers' order IDs, excluded from everything (SPEC 9.4).
	MarkerIDs []string `json:"marker_ids,omitempty"`
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
	RelayBatches  int     `json:"relay_batches"`
	BatchRowsMean float64 `json:"batch_rows_mean"`
	BatchRowsMax  int     `json:"batch_rows_max"`
	// Business holds the per-order damage classes by strategy (SPEC 11.2).
	Business    map[string]Business `json:"business,omitempty"`
	WindowP50MS float64             `json:"window_p50_ms"`
	WindowP99MS float64             `json:"window_p99_ms"`
	WindowMaxMS float64             `json:"window_max_ms"`
	// Commit-time safeguards (SPEC 5.1). When either count is above zero the
	// window is left empty and WindowInvalid is set.
	CommitTSMissing int  `json:"commit_ts_missing"`
	WindowNegative  int  `json:"window_negative"`
	WindowInvalid   bool `json:"window_invalid"`
}

// Business is one strategy's damage classes. Each payment row is in at most one
// row class and each paid order at most once in Missing, so none can cancel
// another (SPEC 11.2).
type Business struct {
	Missing      int `json:"missing_payments"`
	Duplicate    int `json:"duplicate_payments"`
	Unexpected   int `json:"unexpected_payments"`
	WrongAmount  int `json:"incorrect_payment_amounts"`
	WrongMapping int `json:"incorrect_order_mappings"`
}

// Total is business_damage_<strategy>.
func (b Business) Total() int {
	return b.Missing + b.Duplicate + b.Unexpected + b.WrongAmount + b.WrongMapping
}

// BusinessDetail lists the orders and payment rows behind each class.
type BusinessDetail struct {
	Missing      []string `json:"missing_orders"`
	Duplicate    []int64  `json:"duplicate_payment_ids"`
	Unexpected   []int64  `json:"unexpected_payment_ids"`
	WrongAmount  []int64  `json:"wrong_amount_payment_ids"`
	WrongMapping []int64  `json:"wrong_mapping_payment_ids"`
}

// Damage lists the exact pairs behind each metric, for root-cause analysis.
type Damage struct {
	Lost       []Pair      `json:"lost"`
	LostDetail []LostEvent `json:"lost_detail"`
	// LateAfterFailover counts pre-failover versions delivered after it (D vectors).
	LateAfterFailover int `json:"late_after_failover"`
	// SlotTimeline samples logical slots once a second (P4, P5).
	SlotTimeline    []SlotSample `json:"slot_timeline,omitempty"`
	Ghost           []Pair       `json:"ghost"`
	Duplicates      []Pair       `json:"duplicates"` // one entry per extra copy
	OutOfOrder      []Pair       `json:"out_of_order"`
	StaleRedelivery []Pair       `json:"stale_redelivery"`
	LostAfterSend   []Pair       `json:"lost_after_send"`
	LostAfterAck    []Pair       `json:"lost_after_ack"`
	// Versions left out of the window: no commit time, or delivered before it.
	CommitTSMissing []Pair                    `json:"commit_ts_missing,omitempty"`
	WindowNegative  []Pair                    `json:"window_negative,omitempty"`
	Business        map[string]BusinessDetail `json:"business,omitempty"`
}

// SlotSample is one reading of a replication slot.
type SlotSample struct {
	Time           time.Time `json:"time"`
	Slot           string    `json:"slot"`
	Active         bool      `json:"active"`
	ConfirmedFlush string    `json:"confirmed_flush_lsn"`
	Restart        string    `json:"restart_lsn"`
	CurrentWAL     string    `json:"current_wal_lsn"`
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
	in = withoutMarkers(in)
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

	missing := make(map[Pair]bool)
	for _, v := range in.DB {
		if v.CommitMissing {
			missing[v.Pair] = true
		}
	}
	damage.CommitTSMissing = slices.SortedFunc(maps.Keys(missing), comparePairs)
	windows := make([]float64, 0, len(first))
	for _, pair := range sortedKeys(first) {
		commit, ok := committed[pair]
		if !ok || missing[pair] {
			continue
		}
		window := first[pair].Sub(commit)
		if window < -windowTolerance {
			damage.WindowNegative = append(damage.WindowNegative, pair)
			continue
		}
		windows = append(windows, float64(window)/float64(time.Millisecond))
	}
	result.CommitTSMissing = len(damage.CommitTSMissing)
	result.WindowNegative = len(damage.WindowNegative)
	if result.CommitTSMissing+result.WindowNegative > 0 {
		// A wrong timestamp would silently distort the window: leave it empty.
		result.WindowInvalid = true
	} else {
		slices.Sort(windows)
		result.WindowP50MS = obs.Percentile(windows, 50)
		result.WindowP99MS = obs.Percentile(windows, 99)
		result.WindowMaxMS = obs.Percentile(windows, 100)
	}

	if in.Payments != nil {
		result.Business = make(map[string]Business, len(in.Payments))
		damage.Business = make(map[string]BusinessDetail, len(in.Payments))
		resolve := resolver(in.Kafka)
		paidSeq := paidVersions(in.DB, finalVersion)
		for strategy, rows := range in.Payments {
			counts, detail := businessDamage(in.Orders, paidSeq, rows, resolve)
			result.Business[strategy], damage.Business[strategy] = counts, detail
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

// LateDeliveries counts versions committed before the moment whose first copy
// reached Kafka after it: events still in flight when a fault struck.
func LateDeliveries(in Input, moment time.Time) int {
	first := make(map[Pair]time.Time, len(in.Kafka))
	for _, r := range in.Kafka {
		if t, seen := first[r.Pair]; !seen || r.Timestamp.Before(t) {
			first[r.Pair] = r.Timestamp
		}
	}
	late := 0
	for _, v := range in.DB {
		if delivered, ok := first[v.Pair]; ok && v.CommitTime.Before(moment) && delivered.After(moment) {
			late++
		}
	}
	return late
}

func toSet(pairs []Pair) map[Pair]struct{} {
	set := make(map[Pair]struct{}, len(pairs))
	for _, p := range pairs {
		set[p] = struct{}{}
	}
	return set
}

func comparePairs(a, b Pair) int {
	return cmp.Or(cmp.Compare(a.OrderID, b.OrderID), cmp.Compare(a.Seq, b.Seq))
}

func sortedKeys[V any](m map[Pair]V) []Pair {
	return slices.SortedFunc(maps.Keys(m), comparePairs)
}
