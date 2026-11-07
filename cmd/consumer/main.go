// Command consumer applies payments from the event stream using one of three
// strategies (SPEC 10).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"dualwrite-study/internal/config"
	"dualwrite-study/internal/crash"
	"dualwrite-study/internal/events"
	"dualwrite-study/internal/obs"
	"dualwrite-study/internal/store"
)

const (
	poolSize     = 5
	retryDelay   = 500 * time.Millisecond
	operationCap = 10 * time.Second
)

// effect applies a payment and reports whether it changed anything.
type effect func(context.Context, events.Event) (bool, error)

func main() {
	if err := run(); err != nil {
		slog.Error("consumer failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	obs.Setup("consumer-"+cfg.ConsumerStrategy, cfg.Pattern, cfg.CycleID)
	crash.Setup()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	st, err := store.Open(ctx, cfg.PostgresDSN, poolSize)
	if err != nil {
		return err
	}
	defer st.Close()

	apply, err := strategy(cfg.ConsumerStrategy, st)
	if err != nil {
		return err
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.ConsumerGroup(fmt.Sprintf("g-%s-%s", cfg.ConsumerStrategy, cfg.CycleID)),
		kgo.ConsumeTopics(cfg.Topic()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.SessionTimeout(cfg.ConsumerSession),
	)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()

	c := consumer{client: client, apply: apply, offsetFirst: cfg.ConsumerOrder == "offset_first"}
	slog.Info("consumer started", "topic", cfg.Topic(), "strategy", cfg.ConsumerStrategy, "order", cfg.ConsumerOrder)
	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.Canceled) {
				slog.Error("fetch failed", "topic", topic, "partition", partition, "error", err)
			}
		})
		for iter := fetches.RecordIter(); !iter.Done() && ctx.Err() == nil; {
			c.handle(ctx, iter.Next())
		}
	}
	slog.Info("shutdown complete")
	return nil
}

func strategy(name string, st *store.Store) (effect, error) {
	switch name {
	case "naive":
		return st.PayNaive, nil
	case "dedup":
		return st.PayDedup, nil
	case "upsert":
		return st.PayUpsert, nil
	}
	return nil, fmt.Errorf("CONSUMER_STRATEGY must be naive, dedup or upsert, got %q", name)
}

type consumer struct {
	client      *kgo.Client
	apply       effect
	offsetFirst bool
}

// handle processes one record to completion, including during shutdown, so a
// graceful stop never leaves an effect without its commit (SPEC 6.5).
func (c consumer) handle(ctx context.Context, r *kgo.Record) {
	ev, err := events.Decode(r.Value)
	if err != nil {
		slog.Error("undecodable record", "partition", r.Partition, "offset", r.Offset, "error", err)
		c.commit(r)
		return
	}
	if !ev.IsPayment() {
		c.commit(r)
		return
	}
	attrs := []slog.Attr{slog.String("order_id", ev.OrderID.String()), slog.Int("seq", ev.Seq)}
	if c.offsetFirst {
		c.commit(r)
		crash.Point("after_commit_before_side_effect", attrs...)
		c.pay(ctx, r, ev)
		return
	}
	c.pay(ctx, r, ev)
	crash.Point("after_side_effect_before_commit", attrs...)
	c.commit(r)
}

// pay retries until the effect is applied: dropping it would be silent damage.
func (c consumer) pay(ctx context.Context, r *kgo.Record, ev events.Event) {
	for {
		attempt, cancel := context.WithTimeout(context.WithoutCancel(ctx), operationCap)
		applied, err := c.apply(attempt, ev)
		cancel()
		if err == nil {
			action := "skipped"
			if applied {
				action = "applied"
			}
			slog.Info(obs.Consumed, "event_key", ev.Key(), "partition", r.Partition, "offset", r.Offset, "action", action)
			return
		}
		slog.Error("payment failed", "event_key", ev.Key(), "error", err)
		time.Sleep(retryDelay)
	}
}

func (c consumer) commit(r *kgo.Record) {
	ctx, cancel := context.WithTimeout(context.Background(), operationCap)
	defer cancel()
	if err := c.client.CommitRecords(ctx, r); err != nil {
		slog.Error("offset commit failed", "partition", r.Partition, "offset", r.Offset, "error", err)
	}
}
