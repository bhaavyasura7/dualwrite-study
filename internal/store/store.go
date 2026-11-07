// Package store is the Postgres access layer.
package store

import (
	"context"
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
