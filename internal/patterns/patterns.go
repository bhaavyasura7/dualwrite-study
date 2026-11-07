// Package patterns implements the synchronisation patterns P1..P5 (SPEC 6).
package patterns

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"dualwrite-study/internal/config"
	"dualwrite-study/internal/events"
	"dualwrite-study/internal/publish"
	"dualwrite-study/internal/store"
)

// OrderChange is one business change. An empty Status creates the order.
type OrderChange struct {
	OrderID uuid.UUID
	Status  string
	Amount  float64
}

// Pattern applies a change and does whatever publishing the pattern requires.
type Pattern interface {
	Apply(ctx context.Context, change OrderChange) error
}

// New returns the pattern selected by cfg.Pattern.
func New(cfg config.Config, st *store.Store, pub *publish.Publisher) (Pattern, error) {
	dual := dualWrite{store: st, publisher: pub, pattern: cfg.Pattern, cycleID: cfg.CycleID}
	switch cfg.Pattern {
	case "P1":
		return &naive{dual}, nil
	case "P2":
		return &retrying{dualWrite: dual, max: cfg.RetryMax, base: cfg.RetryBase, limit: cfg.RetryCap}, nil
	case "P3", "P4":
		return &outbox{store: st, pattern: cfg.Pattern, cycleID: cfg.CycleID}, nil
	case "P5":
		return &cdc{store: st, pattern: cfg.Pattern, cycleID: cfg.CycleID}, nil
	}
	return nil, fmt.Errorf("unknown pattern %s", cfg.Pattern)
}

func upsertOrder(ctx context.Context, tx pgx.Tx, change OrderChange) (store.Order, error) {
	if change.Status == "" {
		return store.CreateOrder(ctx, tx, change.OrderID, change.Amount)
	}
	return store.ChangeStatus(ctx, tx, change.OrderID, change.Status)
}

// hook identifies an event at a crash point.
func hook(ev events.Event) []slog.Attr {
	return []slog.Attr{
		slog.String("event_id", ev.EventID.String()),
		slog.String("order_id", ev.OrderID.String()),
		slog.Int("seq", ev.Seq),
	}
}
