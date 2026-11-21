// Package store is the Postgres access layer.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"dualwrite-study/internal/events"
)

// ErrNoTransition means the order does not exist or is no longer PLACED.
var ErrNoTransition = errors.New("order not found or not in PLACED status")

// Order is one committed version of an order.
type Order struct {
	ID      uuid.UUID
	Status  string
	Version int
	Amount  float64
}

// Store wraps the connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// Open creates the pool. Connections are made lazily, so a component can start
// while the database is unreachable.
func Open(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Begin starts a transaction.
func (s *Store) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	return tx, nil
}

// CreateOrder inserts version 1 of an order.
func CreateOrder(ctx context.Context, tx pgx.Tx, id uuid.UUID, amount float64) (Order, error) {
	_, err := tx.Exec(ctx,
		`INSERT INTO orders (order_id, status, version, amount) VALUES ($1, $2, 1, $3)`,
		id, events.StatusPlaced, amount)
	if err != nil {
		return Order{}, fmt.Errorf("insert order: %w", err)
	}
	return Order{ID: id, Status: events.StatusPlaced, Version: 1, Amount: amount}, nil
}

// ChangeStatus moves a PLACED order to status and increments its version.
func ChangeStatus(ctx context.Context, tx pgx.Tx, id uuid.UUID, status string) (Order, error) {
	order := Order{ID: id, Status: status}
	err := tx.QueryRow(ctx,
		`UPDATE orders SET status = $2, version = version + 1, updated_at = now()
		 WHERE order_id = $1 AND status = $3
		 RETURNING version, amount::float8`,
		id, status, events.StatusPlaced).Scan(&order.Version, &order.Amount)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNoTransition
	}
	if err != nil {
		return Order{}, fmt.Errorf("update order: %w", err)
	}
	return order, nil
}

// InsertOutbox writes the event in the caller's transaction.
func InsertOutbox(ctx context.Context, tx pgx.Tx, ev events.Event) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (id, aggregatetype, aggregateid, type, seq, payload)
		 VALUES ($1, 'order', $2, $3, $4, $5::jsonb)`,
		ev.EventID, ev.OrderID.String(), ev.Type, ev.Seq, string(payload))
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

// ReadUnsent locks up to limit unsent rows, oldest first. Rows locked by another
// relay are skipped.
func ReadUnsent(ctx context.Context, tx pgx.Tx, limit int) ([]events.Event, error) {
	rows, err := tx.Query(ctx,
		`SELECT payload FROM outbox WHERE sent_at IS NULL ORDER BY created_at, id
		 LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("read outbox: %w", err)
	}
	defer rows.Close()

	var batch []events.Event
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		ev, err := events.Decode(payload)
		if err != nil {
			return nil, err
		}
		batch = append(batch, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read outbox: %w", err)
	}
	return batch, nil
}

// MarkSent records the rows as published.
func MarkSent(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) error {
	if _, err := tx.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return fmt.Errorf("mark sent: %w", err)
	}
	return nil
}

// PayNaive records a payment for every event it sees.
func (s *Store) PayNaive(ctx context.Context, ev events.Event) (bool, error) {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO consumer.payments_naive (order_id, event_key, amount) VALUES ($1, $2, $3)`,
		ev.OrderID, ev.Key(), ev.Amount)
	if err != nil {
		return false, fmt.Errorf("insert payment: %w", err)
	}
	return true, nil
}

// PayDedup records a payment once per event key, in one transaction.
func (s *Store) PayDedup(ctx context.Context, ev events.Event) (bool, error) {
	tx, err := s.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	tag, err := tx.Exec(ctx,
		`INSERT INTO consumer.processed_events (event_key) VALUES ($1) ON CONFLICT DO NOTHING`, ev.Key())
	if err != nil {
		return false, fmt.Errorf("insert processed event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO consumer.payments_dedup (order_id, event_key, amount) VALUES ($1, $2, $3)`,
		ev.OrderID, ev.Key(), ev.Amount)
	if err != nil {
		return false, fmt.Errorf("insert payment: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return true, nil
}

// PayUpsert records at most one payment per order.
func (s *Store) PayUpsert(ctx context.Context, ev events.Event) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO consumer.payments_upsert (order_id, event_key, amount) VALUES ($1, $2, $3)
		 ON CONFLICT (order_id) DO NOTHING`,
		ev.OrderID, ev.Key(), ev.Amount)
	if err != nil {
		return false, fmt.Errorf("upsert payment: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
