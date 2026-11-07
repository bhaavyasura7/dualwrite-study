package patterns

import (
	"context"
	"fmt"

	"dualwrite-study/internal/crash"
	"dualwrite-study/internal/events"
	"dualwrite-study/internal/publish"
	"dualwrite-study/internal/store"
)

// naive is P1: commit, then publish with no safety net.
type naive struct {
	store     *store.Store
	publisher *publish.Publisher
	pattern   string
	cycleID   string
}

func (p *naive) Apply(ctx context.Context, change OrderChange) error {
	tx, err := p.store.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	order, err := upsertOrder(ctx, tx, change)
	if err != nil {
		return err
	}
	ev, err := events.New(order.ID, order.Version, order.Status, order.Amount, p.pattern, p.cycleID)
	if err != nil {
		return err
	}

	crash.Point("before_db_commit", hook(ev)...)
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	crash.Point("after_db_commit", hook(ev)...)

	err = p.publisher.Send(ctx, ev)
	crash.Point("after_send_before_flush", hook(ev)...)
	return err
}
