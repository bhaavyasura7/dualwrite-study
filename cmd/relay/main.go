// Command relay publishes unsent outbox rows in batches (SPEC 8).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"dualwrite-study/internal/config"
	"dualwrite-study/internal/crash"
	"dualwrite-study/internal/obs"
	"dualwrite-study/internal/publish"
	"dualwrite-study/internal/store"
)

// batchTimeout bounds one batch. It is not tied to the stop signal, because a
// stopping relay must finish its batch (SPEC 6.5).
const batchTimeout = 60 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("relay failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	obs.Setup("relay", cfg.Pattern, cfg.CycleID)
	crash.Setup()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	st, err := store.Open(ctx, cfg.PostgresDSN, 2)
	if err != nil {
		return err
	}
	defer st.Close()

	pub, err := publish.New(cfg)
	if err != nil {
		return err
	}
	defer pub.Close()

	slog.Info("relay started", "topic", cfg.Topic(), "batch_size", cfg.RelayBatchSize)
	for ctx.Err() == nil {
		n, err := publishBatch(st, pub, cfg.RelayBatchSize)
		if err != nil {
			slog.Error("batch failed", "error", err)
		}
		if n == 0 || err != nil {
			select {
			case <-ctx.Done():
			case <-time.After(cfg.RelayPollInterval):
			}
		}
	}
	slog.Info("shutdown complete")
	return nil
}

// publishBatch holds its transaction open while publishing, so the row locks
// persist. This is what makes B-2 possible (SPEC 8).
func publishBatch(st *store.Store, pub *publish.Publisher, size int) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), batchTimeout)
	defer cancel()

	tx, err := st.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	batch, err := store.ReadUnsent(ctx, tx, size)
	if err != nil || len(batch) == 0 {
		return 0, err
	}
	batchID := uuid.New()
	ids := make([]uuid.UUID, len(batch))
	hooked := make([]map[string]any, len(batch))
	for i, ev := range batch {
		ids[i] = ev.EventID
		hooked[i] = map[string]any{"order_id": ev.OrderID, "seq": ev.Seq}
	}
	log := func(phase string) {
		slog.Info(obs.RelayBatch, "batch_id", batchID, "n", len(batch), "first_id", ids[0], "last_id", ids[len(ids)-1], "phase", phase)
	}
	attrs := []slog.Attr{slog.String("batch_id", batchID.String()), slog.Any("hooked", hooked)}

	log("read")
	crash.Point("after_read_before_publish", attrs...)
	crash.Stall("after_read_before_publish", attrs[0])

	if err := pub.SendBatch(ctx, batch); err != nil {
		return 0, fmt.Errorf("publish batch: %w", err)
	}
	log("published")
	crash.Point("after_publish_before_mark", attrs...)
	crash.Stall("after_publish_before_mark", attrs[0])

	if err := store.MarkSent(ctx, tx, ids); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	log("marked")
	return len(batch), nil
}
