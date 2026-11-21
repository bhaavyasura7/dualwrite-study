// Package publish wraps the Kafka producer and logs the delivery markers (SPEC 6.4).
package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

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
		kgo.MaxBufferedRecords(cfg.KafkaMaxBuffered),
	}
	if cfg.KafkaRecordRetries > 0 {
		opts = append(opts, kgo.RecordRetries(cfg.KafkaRecordRetries))
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
	if cfg.KafkaTraceAttempts {
		opts = append(opts, kgo.WithHooks(requestTracer{}))
	}
	return opts
}

// requestTracer logs each produce request's lifetime. With SENT_CALLED and
// SEND_ACKED it shows whether a slow record sat in one long request or several.
type requestTracer struct{}

const produceKey = 0

// OnProduceBatchWritten logs each batch written to a partition, so a cut can be
// matched against what each partition had in flight.
func (requestTracer) OnProduceBatchWritten(_ kgo.BrokerMetadata, _ string, partition int32, m kgo.ProduceBatchMetrics) {
	slog.Info(obs.BatchWritten, "partition", partition, "records", m.NumRecords)
}

func (requestTracer) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, e kgo.BrokerE2E) {
	if key != produceKey {
		return
	}
	slog.Info(obs.ProduceTrace,
		"write_wait_ms", e.WriteWait.Milliseconds(),
		"read_wait_ms", e.ReadWait.Milliseconds(),
		"total_ms", (e.WriteWait + e.TimeToWrite + e.ReadWait + e.TimeToRead).Round(time.Millisecond).Milliseconds(),
		"write_err", fmt.Sprint(e.WriteErr),
		"read_err", fmt.Sprint(e.ReadErr))
}

// Send produces ev. In async mode it returns once the record is buffered and
// delivery failures only reach the log. In sync mode it waits for the outcome.
func (p *Publisher) Send(ctx context.Context, ev events.Event) error {
	done := make(chan error, 1)
	if err := p.SendAsync(ctx, ev, func(err error) { done <- err }); err != nil {
		return err
	}
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

// SendAsync produces ev and reports the outcome to done.
func (p *Publisher) SendAsync(ctx context.Context, ev events.Event, done func(error)) error {
	value, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	record := &kgo.Record{Key: []byte(ev.OrderID.String()), Value: value}

	slog.Info(obs.SentCalled, "event_id", ev.EventID, "order_id", ev.OrderID, "seq", ev.Seq)
	// The record must outlive the HTTP request that created it.
	p.client.Produce(context.WithoutCancel(ctx), record, func(r *kgo.Record, err error) {
		if err != nil {
			slog.Error(obs.SendFailed, "event_id", ev.EventID, "partition", r.Partition, "error", err)
		} else {
			slog.Info(obs.SendAcked, "event_id", ev.EventID, "partition", r.Partition, "offset", r.Offset)
		}
		done(err)
	})
	return nil
}

// SendBatch produces every event and waits for all outcomes, returning the first
// error. Records that succeeded stay in Kafka whatever happens to the others.
func (p *Publisher) SendBatch(ctx context.Context, batch []events.Event) error {
	results := make(chan error, len(batch))
	for _, ev := range batch {
		if err := p.SendAsync(ctx, ev, func(err error) { results <- err }); err != nil {
			return err
		}
	}
	var first error
	for range batch {
		if err := <-results; err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Flush waits for all buffered records to be delivered or fail.
func (p *Publisher) Flush(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

// Buffered is the number of records produced but not yet delivered or failed.
func (p *Publisher) Buffered() int64 { return p.client.BufferedProduceRecords() }

// Close releases the client.
func (p *Publisher) Close() { p.client.Close() }
