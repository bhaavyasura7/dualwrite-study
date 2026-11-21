package patterns

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"dualwrite-study/internal/crash"
	"dualwrite-study/internal/events"
	"dualwrite-study/internal/obs"
	"dualwrite-study/internal/publish"
	"dualwrite-study/internal/store"
)

// dualWrite holds what P1 and P2 share: commit first, then publish.
type dualWrite struct {
	store     *store.Store
	publisher *publish.Publisher
	pattern   string
	cycleID   string
}

func (d *dualWrite) commit(ctx context.Context, change OrderChange) (events.Event, error) {
	tx, err := d.store.Begin(ctx)
	if err != nil {
		return events.Event{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	order, err := upsertOrder(ctx, tx, change)
	if err != nil {
		return events.Event{}, err
	}
	ev, err := events.New(order.ID, order.Version, order.Status, order.Amount, d.pattern, d.cycleID)
	if err != nil {
		return events.Event{}, err
	}
	crash.Point("before_db_commit", hook(ev)...)
	if err := tx.Commit(ctx); err != nil {
		return events.Event{}, fmt.Errorf("commit: %w", err)
	}
	crash.Point("after_db_commit", hook(ev)...)
	return ev, nil
}

// naive is P1: commit, then publish with no safety net.
type naive struct {
	dualWrite
}

func (p *naive) Apply(ctx context.Context, change OrderChange) error {
	ev, err := p.commit(ctx, change)
	if err != nil {
		return err
	}
	err = p.publisher.Send(ctx, ev)
	crash.Point("after_send_before_flush", hook(ev)...)
	return err
}

// retrying is P2: as P1, but a failed publish is retried with exponential
// back-off, held in memory only.
type retrying struct {
	dualWrite
	max         int
	base, limit time.Duration
	pending     sync.WaitGroup
	outstanding atomic.Int64 // events not yet delivered or given up, for GET /pending
}

// Pending is the number of events still being delivered or retried. The
// runner's drain waits for it to reach zero (SPEC 9.4).
func (p *retrying) Pending() int64 { return p.outstanding.Load() }

func (p *retrying) done() {
	p.outstanding.Add(-1)
	p.pending.Done()
}

func (p *retrying) Apply(ctx context.Context, change OrderChange) error {
	ev, err := p.commit(ctx, change)
	if err != nil {
		return err
	}
	p.pending.Add(1)
	p.outstanding.Add(1)
	p.deliver(context.WithoutCancel(ctx), ev, 0)
	crash.Point("after_send_before_flush", hook(ev)...)
	return nil
}

func (p *retrying) deliver(ctx context.Context, ev events.Event, attempt int) {
	err := p.publisher.SendAsync(ctx, ev, func(err error) {
		if err == nil {
			p.done()
			return
		}
		p.scheduleRetry(ctx, ev, attempt)
	})
	if err != nil {
		slog.Error(obs.SendFailed, "event_id", ev.EventID, "error", err)
		p.done()
	}
}

func (p *retrying) scheduleRetry(ctx context.Context, ev events.Event, attempt int) {
	if attempt >= p.max {
		slog.Error(obs.RetryExhausted, "event_id", ev.EventID, "order_id", ev.OrderID, "seq", ev.Seq)
		p.done()
		return
	}
	backoff := min(p.base<<attempt, p.limit)
	slog.Info(obs.RetryScheduled, "event_id", ev.EventID, "attempt", attempt+1, "backoff_ms", backoff.Milliseconds())
	if attempt == 0 {
		crash.Point("during_retry_backoff", hook(ev)...)
	}
	time.AfterFunc(backoff, func() { p.deliver(ctx, ev, attempt+1) })
}

// Drain waits for outstanding retries, for graceful shutdown (SPEC 6.5).
func (p *retrying) Drain(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.pending.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("retries still pending: %w", ctx.Err())
	}
}
