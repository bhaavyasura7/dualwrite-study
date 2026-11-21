package checker

import (
	"time"

	"dualwrite-study/internal/raw"
)

// Cost is the database cost over one interval, from the end of warm-up to load
// stop (SPEC 11.2). Known is false when the CPU samples are missing or come from
// different containers (a failover), or when commit times are invalid; the CSV
// columns are then empty.
type Cost struct {
	Known            bool
	DBCPUS           float64 // CPU seconds of postgres-primary in the interval
	WindowS          float64 // the interval's length
	CommittedChanges int     // orders_history rows committed in the interval
	ChangesPerS      float64
	OrdersPerS       float64 // version-1 rows per second
	PerThousand      float64 // DBCPUS / CommittedChanges * 1000
	DBCPUPct         float64 // DBCPUS / WindowS * 100; 100 is one full core
}

// ComputeCost counts committed changes by commit time within the CPU interval.
func ComputeCost(in Input, start, end raw.CPUSample, valid, windowInvalid bool) Cost {
	in = withoutMarkers(in)
	if !valid || windowInvalid || !end.At.After(start.At) || end.TotalNS < start.TotalNS {
		return Cost{}
	}
	c := Cost{Known: true, WindowS: end.At.Sub(start.At).Seconds()}
	c.DBCPUS = float64(end.TotalNS-start.TotalNS) / float64(time.Second)
	orders := 0
	for _, v := range in.DB {
		if v.CommitTime.Before(start.At) || v.CommitTime.After(end.At) {
			continue
		}
		c.CommittedChanges++
		if v.Seq == 1 {
			orders++
		}
	}
	c.ChangesPerS = float64(c.CommittedChanges) / c.WindowS
	c.OrdersPerS = float64(orders) / c.WindowS
	c.DBCPUPct = c.DBCPUS / c.WindowS * 100
	if c.CommittedChanges > 0 {
		c.PerThousand = c.DBCPUS / float64(c.CommittedChanges) * 1000
	}
	return c
}
