// Package publish wraps the Kafka producer and logs the delivery markers (SPEC 6.4).
package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"dualwrite-study/internal/config"
	"dualwrite-study/internal/events"
	"dualwrite-study/internal/obs"
)

// Publisher sends events to the cycle's topic.
type Publisher struct {
	client *kgo.Client
	topic  string
	sync   bool
}

// New creates the producer. Every behaviour that affects results is set
// explicitly, because franz-go's defaults differ from the catalogue.
func New(cfg config.Config) (*Publisher, error) {
	client, err := kgo.NewClient(options(cfg)...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &Publisher{client: client, topic: cfg.Topic(), sync: cfg.KafkaSyncSend}, nil
}

func options(cfg config.Config) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.DefaultProduceTopic(cfg.Topic()),
		kgo.ProducerLinger(cfg.KafkaLinger),
		kgo.ProduceRequestTimeout(cfg.KafkaRequestTimeout),
		kgo.RequestTimeoutOverhead(cfg.KafkaRequestOverhead),
		kgo.RecordDeliveryTimeout(cfg.KafkaDeliveryTimeout),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	}
	if cfg.KafkaAcks == "1" {
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	} else {
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}
	// Idempotent writes require acks=all, so C-4's acks=1 cell runs without them.
	if !cfg.KafkaIdempotent || cfg.KafkaAcks == "1" {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	return opts
}

// Send produces ev. In async mode it returns once the record is buffered and
// delivery failures only reach the log. In sync mode it waits for the outcome.
func (p *Publisher) Send(ctx context.Context, ev events.Event) error {
	value, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	record := &kgo.Record{Key: []byte(ev.OrderID.String()), Value: value}

	done := make(chan error, 1)
	slog.Info(obs.SentCalled, "event_id", ev.EventID, "order_id", ev.OrderID, "seq", ev.Seq)
	// The record must outlive the HTTP request that created it.
	p.client.Produce(context.WithoutCancel(ctx), record, func(r *kgo.Record, err error) {
		if err != nil {
			slog.Error(obs.SendFailed, "event_id", ev.EventID, "error", err)
		} else {
			slog.Info(obs.SendAcked, "event_id", ev.EventID, "partition", r.Partition, "offset", r.Offset)
		}
		done <- err
	})
	if !p.sync {
		return nil
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush waits for all buffered records to be delivered or fail.
func (p *Publisher) Flush(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

// Close releases the client.
func (p *Publisher) Close() { p.client.Close() }
