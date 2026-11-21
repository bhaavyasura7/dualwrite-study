package runner

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"dualwrite-study/internal/events"
)

// maxMarkers bounds the markers committed to reach every partition (SPEC 9.4).
const maxMarkers = 20

// Drain reasons, the first unmet condition when the drain times out (SPEC 9.4).
const (
	reasonConnector  = "connector_not_running"
	reasonSlot       = "slot_inactive"
	reasonMarker     = "marker_missing"
	reasonPartitions = "marker_partitions"
	reasonPending    = "pending_retries"
	reasonOffsets    = "offsets_moving"
)

// markerID is the n-th drain marker's ID, derived from the cycle seed so a
// replay uses the same IDs.
func markerID(seed uint64, n int) uuid.UUID {
	rng := rand.New(rand.NewPCG(seed, seed^uint64(0xd0d0+n)))
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], rng.Uint64())
	binary.BigEndian.PutUint64(b[8:], rng.Uint64())
	b[6] = b[6]&0x0f | 0x40 // version 4
	b[8] = b[8]&0x3f | 0x80 // RFC 4122 variant
	return uuid.UUID(b)
}

// insertMarker commits one drain marker where the pattern's publisher will see
// it: an outbox row for P3 and P4, an orders row for P5. It returns the WAL
// position right after the commit.
func insertMarker(ctx context.Context, conn *pgx.Conn, c Cycle, id uuid.UUID, seed uint64, n int) (string, error) {
	switch c.Pattern {
	case "P3", "P4":
		ev := events.Event{
			EventID: markerID(seed^0xe7e7, n), OrderID: id, Seq: 1, Type: "DrainMarker", Status: "MARKER",
			CreatedAt: time.Now().UTC(), Pattern: c.Pattern, CycleID: c.ID,
		}
		payload, err := json.Marshal(ev)
		if err != nil {
			return "", err
		}
		if _, err := conn.Exec(ctx, `INSERT INTO outbox (id, aggregatetype, aggregateid, type, seq, payload)
			VALUES ($1, 'order', $2, 'DrainMarker', 1, $3::jsonb)`, ev.EventID, id.String(), string(payload)); err != nil {
			return "", fmt.Errorf("insert drain marker: %w", err)
		}
	case "P5":
		if _, err := conn.Exec(ctx, `INSERT INTO orders (order_id, status, version, amount) VALUES ($1, 'MARKER', 1, 0)`, id); err != nil {
			return "", fmt.Errorf("insert drain marker: %w", err)
		}
	default:
		return "", fmt.Errorf("pattern %s has no drain marker", c.Pattern)
	}
	var lsn string
	if err := conn.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsn); err != nil {
		return "", fmt.Errorf("read wal lsn: %w", err)
	}
	return lsn, nil
}

// markerWatch reads the cycle topic as a read_committed consumer would and
// records where each marker landed.
type markerWatch struct {
	client *kgo.Client
	seen   map[string]string // marker ID -> "partition:offset"
	parts  map[int32]bool
	want   map[string]bool
}

func newMarkerWatch(seeds []string, topic string) (*markerWatch, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return nil, fmt.Errorf("marker watch: %w", err)
	}
	return &markerWatch{client: client, seen: map[string]string{}, parts: map[int32]bool{}, want: map[string]bool{}}, nil
}

func (w *markerWatch) close() { w.client.Close() }

// poll reads what has arrived for up to wait.
func (w *markerWatch) poll(ctx context.Context, wait time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	fetches := w.client.PollFetches(ctx)
	fetches.EachRecord(func(r *kgo.Record) {
		ev, err := events.Decode(r.Value)
		if err != nil {
			return
		}
		id := ev.OrderID.String()
		if w.want[id] {
			if _, ok := w.seen[id]; !ok {
				w.seen[id] = fmt.Sprintf("%d:%d", r.Partition, r.Offset)
				w.parts[r.Partition] = true
			}
		}
	})
}

// drainState is what the drain observed, for the row and the timeline.
type drainState struct {
	markerIDs []string
	offsets   map[string]string
	targetLSN string
	slotLSN   string
	reason    string
	status    json.RawMessage
}

// drain ends when every condition of SPEC 14.2 holds: Kafka end offsets, the
// unsent outbox count and the payment count unchanged for quiet_s; for P3 the
// marker in the topic; for P4 and P5 a marker in every partition, the
// connector and its tasks running and the slot active; for P1 and P2 nothing
// left in the producer buffer or waiting for a retry. Stable offsets alone
// never end a P3-P5 drain. On timeout it records the first unmet condition.
func (r *Runner) drain(parent context.Context, run *cycleRun, seed uint64) (drainState, error) {
	cycle := run.cycle
	var state drainState
	ctx, cancel := context.WithTimeout(parent, time.Duration(cycle.Drain.TimeoutS)*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, run.truthDSN)
	if err != nil {
		return state, fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	topic := "orders." + cycle.ID
	partitions, err := strconv.Atoi(cycle.Env["TOPIC_PARTITIONS"])
	if err != nil {
		return state, fmt.Errorf("TOPIC_PARTITIONS: %w", err)
	}
	usesMarkers := cycle.Pattern == "P3" || isCDC(cycle.Pattern)
	var watch *markerWatch
	addMarker := func() error {
		n := len(state.markerIDs)
		id := markerID(seed, n)
		lsn, err := insertMarker(ctx, conn, cycle, id, seed, n)
		if err != nil {
			return err
		}
		state.markerIDs = append(state.markerIDs, id.String())
		watch.want[id.String()] = true
		state.targetLSN = lsn
		slog.Info("drain marker committed", "cycle", cycle.ID, "marker", id, "n", n+1, "wal_lsn", lsn)
		return nil
	}
	if usesMarkers {
		if watch, err = newMarkerWatch(r.seeds(cycle.Topology.Brokers), topic); err != nil {
			return state, err
		}
		defer watch.close()
		if err := addMarker(); err != nil {
			return state, err
		}
	}

	quiet := time.Duration(cycle.Drain.QuietS) * time.Second
	var last [3]int64
	last[0] = -1
	since := time.Now()
	unmet := reasonOffsets
	for ctx.Err() == nil {
		offsets, err := r.admin.ListEndOffsets(ctx, topic)
		if err == nil {
			err = offsets.Error()
		}
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			// A broker that drops out mid-drain is waited for, within the drain's
			// own timeout, and never counts as quiet.
			waited, werr := r.waitReady(ctx, cycle.Topology.Brokers, topic, "DRAIN")
			run.row.BrokerWaitS += waited.Seconds()
			if werr != nil {
				break
			}
			since = time.Now()
			continue
		}
		var current [3]int64
		offsets.Each(func(o kadm.ListedOffset) { current[0] += o.Offset })
		if cycle.Pattern == "P3" {
			if err := conn.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&current[1]); err != nil {
				return state, fmt.Errorf("count unsent: %w", err)
			}
		}
		if len(cycle.Topology.Consumers) > 0 {
			// Consumers may still be catching up after Kafka has gone quiet.
			if err := conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM consumer.payments_naive)
				+ (SELECT count(*) FROM consumer.payments_dedup)
				+ (SELECT count(*) FROM consumer.payments_upsert)`).Scan(&current[2]); err != nil {
				return state, fmt.Errorf("count payments: %w", err)
			}
		}
		if current != last {
			last, since = current, time.Now()
		}

		unmet = ""
		if isCDC(cycle.Pattern) {
			if ok, status := r.connectorsRunning(ctx); !ok {
				unmet, state.status = reasonConnector, status
			} else if !slotsActive(ctx, conn) {
				unmet = reasonSlot
			}
		}
		if usesMarkers && unmet == "" {
			watch.poll(ctx, 200*time.Millisecond)
			lastID := state.markerIDs[len(state.markerIDs)-1]
			_, lastSeen := watch.seen[lastID]
			switch {
			case cycle.Pattern == "P3" && !lastSeen:
				unmet = reasonMarker
			case isCDC(cycle.Pattern) && len(watch.parts) < partitions:
				unmet = reasonMarker
				if lastSeen {
					// The last marker landed in a partition already covered: send another.
					if len(state.markerIDs) >= maxMarkers {
						unmet = reasonPartitions
					} else if err := addMarker(); err != nil {
						return state, err
					}
				}
			}
		}
		if (cycle.Pattern == "P1" || cycle.Pattern == "P2") && unmet == "" {
			if !r.nothingPending(ctx) {
				unmet = reasonPending
			}
		}
		if unmet == "" && (time.Since(since) < quiet || current[1] != 0) {
			unmet = reasonOffsets
		}
		if unmet == "" {
			state.offsets = watchOffsets(watch)
			state.slotLSN = slotLSN(context.WithoutCancel(ctx), conn)
			return state, nil
		}
		if err := sleep(ctx, 300*time.Millisecond); err != nil {
			break
		}
	}
	state.reason = unmet
	state.offsets = watchOffsets(watch)
	state.slotLSN = slotLSN(context.WithoutCancel(parent), conn)
	return state, fmt.Errorf("drain did not finish within %d s: %s", cycle.Drain.TimeoutS, unmet)
}

func watchOffsets(w *markerWatch) map[string]string {
	if w == nil {
		return nil
	}
	return w.seen
}

// connectorsRunning reports whether every connector and every task is RUNNING,
// with Connect's status document for the evidence.
func (r *Runner) connectorsRunning(ctx context.Context) (bool, json.RawMessage) {
	payload, err := r.connectRequest(ctx, http.MethodGet, "/connectors?expand=status", nil)
	if err != nil {
		return false, nil
	}
	return connectorsRunning(payload), payload
}

func connectorsRunning(payload []byte) bool {
	var doc map[string]struct {
		Status struct {
			Connector struct {
				State string `json:"state"`
			} `json:"connector"`
			Tasks []struct {
				State string `json:"state"`
			} `json:"tasks"`
		} `json:"status"`
	}
	if json.Unmarshal(payload, &doc) != nil || len(doc) == 0 {
		return false
	}
	for _, c := range doc {
		if c.Status.Connector.State != "RUNNING" || len(c.Status.Tasks) == 0 {
			return false
		}
		for _, t := range c.Status.Tasks {
			if t.State != "RUNNING" {
				return false
			}
		}
	}
	return true
}

func slotsActive(ctx context.Context, conn *pgx.Conn) bool {
	var active, total int
	err := conn.QueryRow(ctx, `SELECT count(*) FILTER (WHERE active), count(*) FROM pg_replication_slots WHERE slot_type = 'logical'`).Scan(&active, &total)
	return err == nil && total > 0 && active == total
}

func slotLSN(ctx context.Context, conn *pgx.Conn) string {
	var lsns []string
	rows, err := conn.Query(ctx, `SELECT COALESCE(confirmed_flush_lsn::text, '') FROM pg_replication_slots WHERE slot_type = 'logical' ORDER BY slot_name`)
	if err != nil {
		return ""
	}
	defer rows.Close()
	for rows.Next() {
		var lsn string
		if rows.Scan(&lsn) == nil {
			lsns = append(lsns, lsn)
		}
	}
	return strings.Join(lsns, ";")
}

// nothingPending asks the order service whether it still holds records to
// deliver or events waiting for a retry (GET /pending).
func (r *Runner) nothingPending(ctx context.Context) bool {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.baseLoad.Target+"/pending", nil)
	if err != nil {
		return false
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return false
	}
	defer func() { _ = response.Body.Close() }()
	var pending struct {
		Buffered int64 `json:"buffered"`
		Retries  int64 `json:"retries"`
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&pending) != nil {
		return false
	}
	return pending.Buffered == 0 && pending.Retries == 0
}
