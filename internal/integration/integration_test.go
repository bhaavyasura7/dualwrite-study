//go:build integration

// Package integration checks, against the running stack, the assumptions SPEC
// 2.9 rests on (SPEC 17, "Integration (2.9)"). Run with make smoke-v29, which
// make accept includes. The tests use their own rows, far in the past or with
// their own IDs, and delete them; run them before a session, never during one.
package integration

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"dualwrite-study/internal/config"
	"dualwrite-study/internal/events"
	"dualwrite-study/internal/store"
)

const replicaDSN = "postgres://app:app@localhost:55433/shop"

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect %s: %v (is the stack up? make up)", dsn, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func primary(t *testing.T) *pgx.Conn {
	t.Helper()
	m, err := config.LoadMeasurement()
	if err != nil {
		t.Fatal(err)
	}
	return connect(t, m.PostgresDSN)
}

// TestFreezeKeepsCommitTimes: VACUUM (FREEZE) must not change what
// pg_xact_commit_timestamp(xmin) returns for a row (SPEC 5.1).
func TestFreezeKeepsCommitTimes(t *testing.T) {
	ctx := context.Background()
	conn := primary(t)
	if _, err := conn.Exec(ctx, `DROP TABLE IF EXISTS it_freeze; CREATE TABLE it_freeze (id int)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), `DROP TABLE IF EXISTS it_freeze`) })
	for i := range 20 {
		if _, err := conn.Exec(ctx, `INSERT INTO it_freeze VALUES ($1)`, i); err != nil {
			t.Fatal(err)
		}
	}
	read := func() []string {
		rows, err := conn.Query(ctx, `SELECT xmin::text || ' ' || pg_xact_commit_timestamp(xmin)::text FROM it_freeze ORDER BY id`)
		if err != nil {
			t.Fatal(err)
		}
		out, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	before := read()
	if _, err := conn.Exec(ctx, `VACUUM (FREEZE) it_freeze`); err != nil {
		t.Fatal(err)
	}
	after := read()
	if !slices.Equal(before, after) {
		t.Fatalf("commit times changed by VACUUM (FREEZE):\nbefore %v\nafter  %v", before, after)
	}
	if len(before) != 20 || before[0] == "" {
		t.Fatalf("no commit times read: %v", before)
	}
}

// TestReplicaKeepsCommitTimes: the replica reports the primary's commit time
// for a replayed transaction, which is what the truth is after failover (SPEC 5.1).
func TestReplicaKeepsCommitTimes(t *testing.T) {
	ctx := context.Background()
	conn := primary(t)
	replica := connect(t, replicaDSN)
	id := uuid.New()
	if _, err := conn.Exec(ctx, `INSERT INTO orders (order_id, status, version, amount) VALUES ($1, 'PLACED', 1, 1)`, id); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), `DELETE FROM orders WHERE order_id = $1`, id)
		_, _ = conn.Exec(context.Background(), `DELETE FROM orders_history WHERE order_id = $1`, id)
	})
	var want time.Time
	if err := conn.QueryRow(ctx, `SELECT pg_xact_commit_timestamp(xmin) FROM orders_history WHERE order_id = $1`, id).Scan(&want); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		var got *time.Time
		err := replica.QueryRow(ctx, `SELECT pg_xact_commit_timestamp(xmin) FROM orders_history WHERE order_id = $1`, id).Scan(&got)
		if err == nil && got != nil {
			if !got.Equal(want) {
				t.Fatalf("replica commit time %v, primary %v", got, want)
			}
			return
		}
		if err == nil && got == nil {
			t.Fatal("the replica returned no commit time; is track_commit_timestamp on there?")
		}
		if time.Now().After(deadline) {
			t.Fatalf("row never reached the replica: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// outboxRow inserts one unsent outbox row with a fixed created_at.
func outboxRow(t *testing.T, conn *pgx.Conn, order uuid.UUID, seq int, created time.Time) uuid.UUID {
	t.Helper()
	ev := events.Event{EventID: uuid.New(), OrderID: order, Seq: seq, Type: "OrderPlaced", Status: "PLACED", Amount: 1}
	payload, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(context.Background(), `INSERT INTO outbox (id, aggregatetype, aggregateid, type, seq, payload, created_at)
		VALUES ($1, 'order', $2, 'OrderPlaced', $3, $4::jsonb, $5)`, ev.EventID, order.String(), seq, string(payload), created); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), `DELETE FROM outbox WHERE id = $1`, ev.EventID) })
	return ev.EventID
}

// TestRelayTieOrder: rows with equal created_at come back in id order (SPEC 8).
func TestRelayTieOrder(t *testing.T) {
	ctx := context.Background()
	conn := primary(t)
	created := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) // before any real row
	var ids []string
	for range 6 {
		ids = append(ids, outboxRow(t, conn, uuid.New(), 1, created).String())
	}
	slices.Sort(ids)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	batch, err := store.ReadUnsent(ctx, tx, len(ids))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, ev := range batch {
		got = append(got, ev.EventID.String())
	}
	if !slices.Equal(got, ids) {
		t.Fatalf("equal created_at not in id order:\n got %v\nwant %v", got, ids)
	}
}

// TestSkipLockedLetsSecondRelayOvertake shows the B-1 mechanism at the database
// level (SPEC 8): while one relay holds an order's version 1, a second relay
// reads version 2 of the same order, so it can publish it first.
func TestSkipLockedLetsSecondRelayOvertake(t *testing.T) {
	ctx := context.Background()
	first := primary(t)
	second := primary(t)
	order := uuid.New()
	base := time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC)
	v1 := outboxRow(t, first, order, 1, base)
	v2 := outboxRow(t, first, order, 2, base.Add(time.Second))

	tx1, err := first.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx1.Rollback(ctx) }()
	held, err := store.ReadUnsent(ctx, tx1, 1)
	if err != nil || len(held) != 1 || held[0].EventID != v1 {
		t.Fatalf("relay 1 read %v, %v; want version 1", held, err)
	}
	tx2, err := second.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx2.Rollback(ctx) }()
	next, err := store.ReadUnsent(ctx, tx2, 1)
	if err != nil || len(next) != 1 || next[0].EventID != v2 {
		t.Fatalf("relay 2 read %v, %v; want version 2 while version 1 is locked", next, err)
	}
}
