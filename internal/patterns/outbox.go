package patterns

import (
	"context"
	"fmt"

	"dualwrite-study/internal/crash"
	"dualwrite-study/internal/events"
	"dualwrite-study/internal/store"
)

// outbox is P3 and P4: the event is written in the same transaction, and a relay
// or Debezium publishes it.
type outbox struct {
	store   *store.Store
	pattern string
	cycleID string
}

func (p *outbox) Apply(ctx context.Context, change OrderChange) error {
	return commitChange(ctx, p.store, change, p.pattern, p.cycleID, true)
}

// cdc is P5: only the business table is written, and Debezium streams it.
type cdc struct {
	store   *store.Store
	pattern string
	cycleID string
}

func (p *cdc) Apply(ctx context.Context, change OrderChange) error {
	return commitChange(ctx, p.store, change, p.pattern, p.cycleID, false)
}

func commitChange(ctx context.Context, st *store.Store, change OrderChange, pattern, cycleID string, writeOutbox bool) error {
	tx, err := st.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	order, err := upsertOrder(ctx, tx, change)
	if err != nil {
		return err
	}
	ev, err := events.New(order.ID, order.Version, order.Status, order.Amount, pattern, cycleID)
	if err != nil {
		return err
	}
	if writeOutbox {
		if err := store.InsertOutbox(ctx, tx, ev); err != nil {
			return err
		}
	}
	crash.Point("before_db_commit", hook(ev)...)
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	crash.Point("after_db_commit", hook(ev)...)
	return nil
}
