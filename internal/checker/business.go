package checker

import (
	"cmp"
	"slices"
	"strconv"
	"strings"

	"dualwrite-study/internal/events"
)

// withoutMarkers drops the drain markers (SPEC 9.4) from every input, so the
// checker measures only the cycle's own load.
func withoutMarkers(in Input) Input {
	if len(in.MarkerIDs) == 0 {
		return in
	}
	marker := make(map[string]bool, len(in.MarkerIDs))
	for _, id := range in.MarkerIDs {
		marker[id] = true
	}
	in.DB = slices.DeleteFunc(slices.Clone(in.DB), func(v Version) bool { return marker[v.OrderID] })
	in.Kafka = slices.DeleteFunc(slices.Clone(in.Kafka), func(r Record) bool { return marker[r.OrderID] })
	in.Orders = slices.DeleteFunc(slices.Clone(in.Orders), func(o Order) bool { return marker[o.OrderID] })
	if in.Payments != nil {
		payments := make(map[string][]Payment, len(in.Payments))
		for strategy, rows := range in.Payments {
			payments[strategy] = slices.DeleteFunc(slices.Clone(rows), func(p Payment) bool { return marker[p.OrderID] })
		}
		in.Payments = payments
	}
	return in
}

// resolver maps an event key to the version it names. For P1–P4 the key is an
// event ID, looked up in the cycle's Kafka records; for P5 it is
// "<order_id>:<seq>" (SPEC 10). An unknown key does not resolve.
func resolver(records []Record) func(key string) (Pair, bool) {
	byEvent := make(map[string]Pair, len(records))
	for _, r := range records {
		if r.EventID != "" {
			byEvent[r.EventID] = r.Pair
		}
	}
	return func(key string) (Pair, bool) {
		if pair, ok := byEvent[key]; ok {
			return pair, true
		}
		order, seq, found := strings.Cut(key, ":")
		if !found {
			return Pair{}, false
		}
		n, err := strconv.Atoi(seq)
		if err != nil || n < 1 {
			return Pair{}, false
		}
		return Pair{OrderID: order, Seq: n}, true
	}
}

// paidVersions is each order's PAID version. Versions without a status (test
// fixtures) fall back to the order's final version, which for this load is the
// PAID one whenever the order is paid (an order is paid or cancelled, never both).
func paidVersions(db []Version, final map[string]int) map[string]int {
	paid := make(map[string]int)
	for _, v := range db {
		if v.Status == events.StatusPaid {
			paid[v.OrderID] = v.Seq
		}
	}
	for order, seq := range final {
		if _, ok := paid[order]; !ok {
			paid[order] = seq
		}
	}
	return paid
}

// businessDamage classifies one strategy's payment rows against the expected
// effects: exactly one payment, of the order's amount, for each order whose
// final status is PAID (SPEC 11.2). Rows are taken in payment_id order and each
// falls into the first class that applies.
func businessDamage(orders []Order, paidSeq map[string]int, rows []Payment, resolve func(string) (Pair, bool)) (Business, BusinessDetail) {
	truth := make(map[string]Order, len(orders))
	for _, o := range orders {
		truth[o.OrderID] = o
	}
	sorted := slices.Clone(rows)
	slices.SortFunc(sorted, func(a, b Payment) int { return cmp.Compare(a.PaymentID, b.PaymentID) })

	var detail BusinessDetail
	correct := make(map[string]bool)
	for _, r := range sorted {
		o, ok := truth[r.OrderID]
		switch {
		case !ok || o.Status != events.StatusPaid:
			detail.Unexpected = append(detail.Unexpected, r.PaymentID)
		case !mapsTo(resolve, r, paidSeq[r.OrderID]):
			detail.WrongMapping = append(detail.WrongMapping, r.PaymentID)
		case r.AmountCents != o.AmountCents:
			detail.WrongAmount = append(detail.WrongAmount, r.PaymentID)
		case correct[r.OrderID]:
			detail.Duplicate = append(detail.Duplicate, r.PaymentID)
		default:
			correct[r.OrderID] = true
		}
	}
	for _, o := range orders {
		if o.Status == events.StatusPaid && !correct[o.OrderID] {
			detail.Missing = append(detail.Missing, o.OrderID)
		}
	}
	slices.Sort(detail.Missing)
	return Business{
		Missing:      len(detail.Missing),
		Duplicate:    len(detail.Duplicate),
		Unexpected:   len(detail.Unexpected),
		WrongAmount:  len(detail.WrongAmount),
		WrongMapping: len(detail.WrongMapping),
	}, detail
}

func mapsTo(resolve func(string) (Pair, bool), r Payment, paidSeq int) bool {
	pair, ok := resolve(r.EventKey)
	return ok && pair.OrderID == r.OrderID && pair.Seq == paidSeq
}

// ParseCents converts a NUMERIC(12,2) value written as text ("499.00", "-1.5",
// "12") to cents exactly, without floating point.
func ParseCents(s string) (int64, error) {
	negative := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	whole, frac, _ := strings.Cut(s, ".")
	if len(frac) > 2 || whole == "" {
		return 0, strconv.ErrSyntax
	}
	frac = (frac + "00")[:2]
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, err
	}
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, err
	}
	cents := w*100 + f
	if negative {
		cents = -cents
	}
	return cents, nil
}
