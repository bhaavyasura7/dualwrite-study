package checker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"dualwrite-study/internal/dump"
	"dualwrite-study/internal/events"
	"dualwrite-study/internal/obs"
	"dualwrite-study/internal/raw"
)

// logAppendTime is the record timestamp type set by the broker.
const logAppendTime = 1

// paymentTables maps consumer strategies to their tables.
var paymentTables = map[string]string{
	"naive":  "consumer.payments_naive",
	"dedup":  "consumer.payments_dedup",
	"upsert": "consumer.payments_upsert",
}

// CollectTruth reads the truth database's orders and every committed version
// with its transaction's xmin and commit time (SPEC 5.1, 11.5). A NULL commit
// time is kept as nil, never guessed.
func CollectTruth(ctx context.Context, conn *pgx.Conn, source string) (raw.Truth, error) {
	truth := raw.Truth{Source: source}
	if err := conn.QueryRow(ctx, `SELECT age(datfrozenxid) FROM pg_database WHERE datname = current_database()`).Scan(&truth.XIDAge); err != nil {
		return raw.Truth{}, fmt.Errorf("read xid age: %w", err)
	}
	rows, err := conn.Query(ctx, `SELECT order_id::text, status, version, amount::text FROM orders ORDER BY order_id`)
	if err != nil {
		return raw.Truth{}, fmt.Errorf("query orders: %w", err)
	}
	for rows.Next() {
		var o raw.Order
		if err := rows.Scan(&o.OrderID, &o.Status, &o.Version, &o.Amount); err != nil {
			rows.Close()
			return raw.Truth{}, fmt.Errorf("scan orders: %w", err)
		}
		truth.Orders = append(truth.Orders, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return raw.Truth{}, fmt.Errorf("read orders: %w", err)
	}
	rows, err = conn.Query(ctx, `SELECT order_id::text, version, status, xmin::text::bigint, pg_xact_commit_timestamp(xmin)
		FROM orders_history ORDER BY order_id, version`)
	if err != nil {
		return raw.Truth{}, fmt.Errorf("query history: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var h raw.History
		var xmin int64
		if err := rows.Scan(&h.OrderID, &h.Version, &h.Status, &xmin, &h.CommitTime); err != nil {
			return raw.Truth{}, fmt.Errorf("scan history: %w", err)
		}
		h.XMin = uint32(xmin)
		truth.History = append(truth.History, h)
	}
	if err := rows.Err(); err != nil {
		return raw.Truth{}, fmt.Errorf("read history: %w", err)
	}
	return truth, nil
}

// CollectOutbox reads every outbox row (P3, P4).
func CollectOutbox(ctx context.Context, conn *pgx.Conn) ([]raw.OutboxRow, error) {
	rows, err := conn.Query(ctx, `SELECT id::text, aggregateid, type, seq, created_at, sent_at FROM outbox ORDER BY created_at, id`)
	if err != nil {
		return nil, fmt.Errorf("query outbox: %w", err)
	}
	defer rows.Close()
	var out []raw.OutboxRow
	for rows.Next() {
		var r raw.OutboxRow
		if err := rows.Scan(&r.ID, &r.AggregateID, &r.Type, &r.Seq, &r.CreatedAt, &r.SentAt); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CollectConsumer reads every row of the strategies' payment tables and the
// processed-event keys (group G).
func CollectConsumer(ctx context.Context, conn *pgx.Conn, strategies []string) (raw.Consumer, error) {
	out := raw.Consumer{Payments: make(map[string][]raw.Payment, len(strategies))}
	for _, strategy := range strategies {
		table, ok := paymentTables[strategy]
		if !ok {
			return raw.Consumer{}, fmt.Errorf("unknown consumer strategy %q", strategy)
		}
		rows, err := conn.Query(ctx, "SELECT payment_id, order_id::text, event_key, amount::text FROM "+table+" ORDER BY payment_id")
		if err != nil {
			return raw.Consumer{}, fmt.Errorf("query %s: %w", table, err)
		}
		payments := []raw.Payment{}
		for rows.Next() {
			var p raw.Payment
			if err := rows.Scan(&p.PaymentID, &p.OrderID, &p.EventKey, &p.Amount); err != nil {
				rows.Close()
				return raw.Consumer{}, fmt.Errorf("scan %s: %w", table, err)
			}
			payments = append(payments, p)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return raw.Consumer{}, fmt.Errorf("read %s: %w", table, err)
		}
		out.Payments[strategy] = payments
	}
	rows, err := conn.Query(ctx, `SELECT event_key FROM consumer.processed_events ORDER BY event_key`)
	if err != nil {
		return raw.Consumer{}, fmt.Errorf("query processed events: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return raw.Consumer{}, fmt.Errorf("scan processed events: %w", err)
		}
		out.Processed = append(out.Processed, key)
	}
	return out, rows.Err()
}

// FromTruth converts the truth into the checker's versions and orders.
func FromTruth(t raw.Truth) ([]Version, []Order, error) {
	versions := make([]Version, 0, len(t.History))
	for _, h := range t.History {
		v := Version{Pair: Pair{OrderID: h.OrderID, Seq: h.Version}, Status: h.Status}
		if h.CommitTime == nil {
			v.CommitMissing = true
		} else {
			v.CommitTime = *h.CommitTime
		}
		versions = append(versions, v)
	}
	orders := make([]Order, 0, len(t.Orders))
	for _, o := range t.Orders {
		cents, err := ParseCents(o.Amount)
		if err != nil {
			return nil, nil, fmt.Errorf("order %s amount %q: %w", o.OrderID, o.Amount, err)
		}
		orders = append(orders, Order{OrderID: o.OrderID, Status: o.Status, AmountCents: cents})
	}
	return versions, orders, nil
}

// FromConsumer converts the payment tables into the checker's payments.
func FromConsumer(c raw.Consumer) (map[string][]Payment, error) {
	out := make(map[string][]Payment, len(c.Payments))
	for strategy, rows := range c.Payments {
		payments := make([]Payment, 0, len(rows))
		for _, p := range rows {
			cents, err := ParseCents(p.Amount)
			if err != nil {
				return nil, fmt.Errorf("%s payment %d amount %q: %w", strategy, p.PaymentID, p.Amount, err)
			}
			payments = append(payments, Payment{PaymentID: p.PaymentID, OrderID: p.OrderID, EventKey: p.EventKey, AmountCents: cents})
		}
		out[strategy] = payments
	}
	return out, nil
}

// FromDump derives the committed records from a raw topic dump, exactly as a
// read_committed consumer reading to the last stable offset sees them, and
// decodes them. It refuses records without a broker timestamp, like ReadKafka.
func FromDump(records []dump.Record) ([]Record, error) {
	committed, err := dump.Committed(records)
	if err != nil {
		return nil, err
	}
	out := make([]Record, 0, len(committed))
	for _, r := range committed {
		if r.TimestampType != logAppendTime {
			return nil, fmt.Errorf("partition %d offset %d: not LogAppendTime", r.Partition, r.Offset)
		}
		rec, err := decodeRecord(r.Partition, r.Offset, r.Timestamp(), r.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

func decodeRecord(partition int32, offset int64, ts time.Time, value []byte) (Record, error) {
	ev, err := events.Decode(value)
	if err != nil {
		return Record{}, fmt.Errorf("partition %d offset %d: %w", partition, offset, err)
	}
	rec := Record{Pair: Pair{OrderID: ev.OrderID.String(), Seq: ev.Seq}, Partition: partition, Offset: offset, Timestamp: ts}
	if ev.EventID != uuid.Nil {
		rec.EventID = ev.EventID.String()
	}
	return rec, nil
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
			rec, err := decodeRecord(r.Partition, r.Offset, r.Timestamp, r.Value)
			if err != nil {
				return nil, err
			}
			records = append(records, rec)
		}
	}
	return records, nil
}

// Evidence is what the cycle's logs contribute to the check.
type Evidence struct {
	Sent            []Pair
	Acked           []Pair
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
	pairs := make(map[string]Pair) // event ID to pair; SEND_ACKED names only the event
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
			pairs[line.EventID] = pair
		case obs.SendAcked:
			delete(pending, line.EventID)
			if acked, ok := pairs[line.EventID]; ok {
				evidence.Acked = append(evidence.Acked, acked)
			}
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

// Collect gathers one cycle's input live from Postgres, Kafka and its logs. The
// runner instead saves raw files and uses FromRaw; Collect serves the checker
// command and the smoke tests, and shares every conversion with FromRaw.
func Collect(ctx context.Context, dsn string, brokers []string, topic string, logs io.Reader, strategies []string) (Input, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return Input{}, fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	var in Input
	truth, err := CollectTruth(ctx, conn, "primary")
	if err != nil {
		return Input{}, err
	}
	if in.DB, in.Orders, err = FromTruth(truth); err != nil {
		return Input{}, err
	}
	if len(strategies) > 0 {
		consumer, err := CollectConsumer(ctx, conn, strategies)
		if err != nil {
			return Input{}, err
		}
		if in.Payments, err = FromConsumer(consumer); err != nil {
			return Input{}, err
		}
	}
	if in.Kafka, err = ReadKafka(ctx, brokers, topic); err != nil {
		return Input{}, err
	}
	if logs != nil {
		if err := addEvidence(&in, logs); err != nil {
			return Input{}, err
		}
	}
	return in, nil
}

func addEvidence(in *Input, logs io.Reader) error {
	evidence, err := ParseLogs(logs)
	if err != nil {
		return err
	}
	in.Sent, in.Acked, in.Hooked, in.SendFailed = evidence.Sent, evidence.Acked, evidence.Hooked, evidence.SendFailed
	in.InflightAtCrash, in.OldestPendingMS = evidence.InflightAtCrash, evidence.OldestPendingMS
	in.BatchRows = evidence.BatchRows
	return nil
}

// Raw is one cycle's raw files, loaded.
type Raw struct {
	Input    Input
	Timeline raw.Timeline
	Load     raw.Load
	Truth    raw.Truth
}

// FromRaw loads a cycle's input from its raw files and dump alone (SPEC 11.5).
// The runner's CHECK and cmd/recompute both use it, so they cannot differ.
func FromRaw(sessionDir, cycleID string) (Raw, error) {
	dir := raw.Dir(sessionDir, cycleID)
	var out Raw
	if err := raw.ReadJSON(dir, raw.TimelineFile, &out.Timeline); err != nil {
		return Raw{}, err
	}
	if err := raw.ReadJSON(dir, raw.TruthFile, &out.Truth); err != nil {
		return Raw{}, err
	}
	if err := raw.ReadJSON(dir, raw.LoadFile, &out.Load); err != nil {
		return Raw{}, err
	}
	in := &out.Input
	var err error
	if in.DB, in.Orders, err = FromTruth(out.Truth); err != nil {
		return Raw{}, err
	}
	records, err := dump.Read(raw.DumpPath(sessionDir, cycleID))
	if err != nil {
		return Raw{}, err
	}
	if in.Kafka, err = FromDump(records); err != nil {
		return Raw{}, err
	}
	if len(out.Timeline.Consumers) > 0 {
		var consumer raw.Consumer
		if err := raw.ReadJSON(dir, raw.ConsumerFile, &consumer); err != nil {
			return Raw{}, err
		}
		if in.Payments, err = FromConsumer(consumer); err != nil {
			return Raw{}, err
		}
	}
	markers, err := raw.ReadBytes(dir, raw.MarkersFile)
	if err != nil {
		return Raw{}, err
	}
	if err := addEvidence(in, bytes.NewReader(markers)); err != nil {
		return Raw{}, err
	}
	in.MarkerIDs = out.Timeline.MarkerIDs
	return out, nil
}

// markerNames are the log markers the checker reads (SPEC 13), plus those kept
// as evidence.
var markerNames = map[string]bool{
	obs.SentCalled: true, obs.SendAcked: true, obs.SendFailed: true, obs.RelayBatch: true,
	obs.CrashPointHit: true, obs.StallPointHit: true, obs.StallPointEnd: true,
	obs.RetryScheduled: true, obs.RetryExhausted: true, obs.Consumed: true, obs.Armed: true,
}

// MarkerLines keeps the JSON log lines that carry a marker, in order, for the
// markers file. ParseLogs reads the result exactly as it reads full logs.
func MarkerLines(logs []byte) ([]byte, error) {
	var out bytes.Buffer
	scanner := bufio.NewScanner(bytes.NewReader(logs))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var line struct {
			Msg string `json:"msg"`
		}
		if json.Unmarshal(scanner.Bytes(), &line) == nil && markerNames[line.Msg] {
			out.Write(scanner.Bytes())
			out.WriteByte('\n')
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read logs: %w", err)
	}
	return out.Bytes(), nil
}
