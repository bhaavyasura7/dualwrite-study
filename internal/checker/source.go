package checker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"dualwrite-study/internal/events"
	"dualwrite-study/internal/obs"
)

// logAppendTime is the record timestamp type set by the broker.
const logAppendTime = 1

// paymentTables maps consumer strategies to their tables.
var paymentTables = map[string]string{
	"naive":  "consumer.payments_naive",
	"dedup":  "consumer.payments_dedup",
	"upsert": "consumer.payments_upsert",
}

// ReadDB loads every committed version with its true commit time, and the
// number of orders whose final status is PAID.
func ReadDB(ctx context.Context, conn *pgx.Conn) ([]Version, int, error) {
	rows, err := conn.Query(ctx,
		`SELECT order_id::text, version, pg_xact_commit_timestamp(xmin) FROM orders_history`)
	if err != nil {
		return nil, 0, fmt.Errorf("query history: %w", err)
	}
	defer rows.Close()

	var versions []Version
	for rows.Next() {
		var v Version
		var commit *time.Time
		if err := rows.Scan(&v.OrderID, &v.Seq, &commit); err != nil {
			return nil, 0, fmt.Errorf("scan history: %w", err)
		}
		if commit == nil {
			return nil, 0, fmt.Errorf("no commit timestamp for %s:%d; is track_commit_timestamp on?", v.OrderID, v.Seq)
		}
		v.CommitTime = *commit
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("read history: %w", err)
	}

	var paid int
	err = conn.QueryRow(ctx, `SELECT count(*) FROM orders WHERE status = $1`, events.StatusPaid).Scan(&paid)
	if err != nil {
		return nil, 0, fmt.Errorf("count paid orders: %w", err)
	}
	return versions, paid, nil
}

// ReadPayments counts the payment rows of each strategy.
func ReadPayments(ctx context.Context, conn *pgx.Conn, strategies []string) (map[string]int, error) {
	payments := make(map[string]int, len(strategies))
	for _, strategy := range strategies {
		table, ok := paymentTables[strategy]
		if !ok {
			return nil, fmt.Errorf("unknown consumer strategy %q", strategy)
		}
		var n int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			return nil, fmt.Errorf("count %s: %w", table, err)
		}
		payments[strategy] = n
	}
	return payments, nil
}

// ReadKafka reads every committed record of topic. It fails rather than return
// a partial log, and refuses records without a broker timestamp, because the
// window metric would then silently measure the producer's clock.
func ReadKafka(ctx context.Context, brokers []string, topic string) ([]Record, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// Transaction markers occupy offsets. Seeing them is how the read
		// knows it reached the end of a partition that ends in one.
		kgo.KeepControlRecords(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()

	ends, err := kadm.NewClient(client).ListCommittedOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	if err := ends.Error(); err != nil {
		return nil, fmt.Errorf("list end offsets of %s: %w", topic, err)
	}
	remaining := make(map[int32]int64)
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			remaining[o.Partition] = o.Offset
		}
	})

	var records []Record
	for len(remaining) > 0 {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err0(); err != nil {
			return nil, fmt.Errorf("fetch %s (%d partitions unfinished): %w", topic, len(remaining), err)
		}
		var firstErr error
		fetches.EachError(func(_ string, partition int32, err error) {
			firstErr = errors.Join(firstErr, fmt.Errorf("partition %d: %w", partition, err))
		})
		if firstErr != nil {
			return nil, fmt.Errorf("fetch %s: %w", topic, firstErr)
		}
		for iter := fetches.RecordIter(); !iter.Done(); {
			r := iter.Next()
			end, reading := remaining[r.Partition]
			if !reading || r.Offset >= end {
				continue
			}
			if r.Offset == end-1 {
				delete(remaining, r.Partition)
			}
			if r.Attrs.IsControl() {
				continue
			}
			if r.Attrs.TimestampType() != logAppendTime {
				return nil, fmt.Errorf("topic %s does not use LogAppendTime", topic)
			}
			ev, err := events.Decode(r.Value)
			if err != nil {
				return nil, fmt.Errorf("partition %d offset %d: %w", r.Partition, r.Offset, err)
			}
			records = append(records, Record{
				Pair:      Pair{OrderID: ev.OrderID.String(), Seq: ev.Seq},
				Partition: r.Partition,
				Offset:    r.Offset,
				Timestamp: r.Timestamp,
			})
		}
	}
	return records, nil
}

// Evidence is what the cycle's logs contribute to the check.
type Evidence struct {
	Sent            []Pair
	Hooked          []Pair
	SendFailed      int
	InflightAtCrash int     // records sent but not yet resolved at the crash marker
	OldestPendingMS float64 // age of the oldest of them
	BatchRows       []int   // rows per relay batch
}

// ParseLogs extracts the markers from a component's JSON log, in log order. Lines
// that are not JSON, such as a Go panic trace, are skipped.
func ParseLogs(r io.Reader) (Evidence, error) {
	var evidence Evidence
	pending := make(map[string]time.Time)
	crashed := false
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var line struct {
			Time    time.Time  `json:"time"`
			Msg     string     `json:"msg"`
			EventID string     `json:"event_id"`
			OrderID string     `json:"order_id"`
			Seq     int        `json:"seq"`
			Phase   string     `json:"phase"`
			N       int        `json:"n"`
			Hooked  []struct { // a whole relay batch
				OrderID string `json:"order_id"`
				Seq     int    `json:"seq"`
			} `json:"hooked"`
		}
		if json.Unmarshal(scanner.Bytes(), &line) != nil {
			continue
		}
		pair := Pair{OrderID: line.OrderID, Seq: line.Seq}
		switch line.Msg {
		case obs.SentCalled:
			evidence.Sent = append(evidence.Sent, pair)
			pending[line.EventID] = line.Time
		case obs.SendAcked:
			delete(pending, line.EventID)
		case obs.RelayBatch:
			if line.Phase == "read" {
				evidence.BatchRows = append(evidence.BatchRows, line.N)
			}
		case obs.SendFailed:
			evidence.SendFailed++
			delete(pending, line.EventID)
		case obs.CrashPointHit:
			if line.OrderID != "" {
				evidence.Hooked = append(evidence.Hooked, pair)
			}
			for _, h := range line.Hooked {
				evidence.Hooked = append(evidence.Hooked, Pair{OrderID: h.OrderID, Seq: h.Seq})
			}
			if !crashed {
				crashed = true
				evidence.InflightAtCrash = len(pending)
				for _, sent := range pending {
					age := float64(line.Time.Sub(sent)) / float64(time.Millisecond)
					evidence.OldestPendingMS = max(evidence.OldestPendingMS, age)
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return Evidence{}, fmt.Errorf("read logs: %w", err)
	}
	return evidence, nil
}

// Collect gathers one cycle's input from Postgres, Kafka and its logs.
func Collect(ctx context.Context, dsn string, brokers []string, topic string, logs io.Reader, strategies []string) (Input, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return Input{}, fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	var in Input
	if in.DB, in.PaidOrders, err = ReadDB(ctx, conn); err != nil {
		return Input{}, err
	}
	if len(strategies) > 0 {
		if in.Payments, err = ReadPayments(ctx, conn, strategies); err != nil {
			return Input{}, err
		}
	}
	if in.Kafka, err = ReadKafka(ctx, brokers, topic); err != nil {
		return Input{}, err
	}
	if logs != nil {
		evidence, err := ParseLogs(logs)
		if err != nil {
			return Input{}, err
		}
		in.Sent, in.Hooked, in.SendFailed = evidence.Sent, evidence.Hooked, evidence.SendFailed
		in.InflightAtCrash, in.OldestPendingMS = evidence.InflightAtCrash, evidence.OldestPendingMS
		in.BatchRows = evidence.BatchRows
	}
	return in, nil
}
