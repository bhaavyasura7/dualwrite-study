// Package events defines the event schema shared by every pattern (SPEC 5.2).
package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Order statuses.
const (
	StatusPlaced    = "PLACED"
	StatusPaid      = "PAID"
	StatusCancelled = "CANCELLED"
)

// Event is the Kafka record value for P1–P4.
type Event struct {
	EventID   uuid.UUID `json:"event_id"`
	OrderID   uuid.UUID `json:"order_id"`
	Seq       int       `json:"seq"`
	Type      string    `json:"type"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	Pattern   string    `json:"pattern"`
	CycleID   string    `json:"cycle_id"`
}

// New builds the event for one committed order version.
func New(orderID uuid.UUID, seq int, status string, amount float64, pattern, cycleID string) (Event, error) {
	typ, err := typeOf(status)
	if err != nil {
		return Event{}, err
	}
	return Event{
		EventID:   uuid.New(),
		OrderID:   orderID,
		Seq:       seq,
		Type:      typ,
		Amount:    amount,
		Status:    status,
		CreatedAt: time.Now().UTC(),
		Pattern:   pattern,
		CycleID:   cycleID,
	}, nil
}

func typeOf(status string) (string, error) {
	switch status {
	case StatusPlaced:
		return "OrderPlaced", nil
	case StatusPaid:
		return "OrderPaid", nil
	case StatusCancelled:
		return "OrderCancelled", nil
	}
	return "", fmt.Errorf("unknown order status %q", status)
}

// Decode parses a Kafka record value: the common schema for P1–P4, or a Debezium
// row-change envelope for P5, mapped from `after`. A value missing its identity
// is an error, so a malformed record can never pass as damage-free.
func Decode(value []byte) (Event, error) {
	var envelope struct {
		Event
		After *struct {
			OrderID uuid.UUID `json:"order_id"`
			Version int       `json:"version"`
			Status  string    `json:"status"`
			Amount  float64   `json:"amount"`
		} `json:"after"`
	}
	if err := json.Unmarshal(value, &envelope); err != nil {
		return Event{}, fmt.Errorf("decode event: %w", err)
	}
	ev := envelope.Event
	if after := envelope.After; after != nil {
		ev = Event{OrderID: after.OrderID, Seq: after.Version, Status: after.Status, Amount: after.Amount, Pattern: "P5"}
	}
	if ev.OrderID == uuid.Nil || ev.Seq < 1 {
		return Event{}, fmt.Errorf("decode event: missing order_id or seq in %q", value)
	}
	return ev, nil
}
