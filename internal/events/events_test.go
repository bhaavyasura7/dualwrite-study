package events

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestEventJSONHasSpecFields(t *testing.T) {
	ev, err := New(uuid.New(), 3, StatusCancelled, 499, "P3", "B-1_P3_r07")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"event_id", "order_id", "seq", "type", "amount", "status", "created_at", "pattern", "cycle_id"} {
		if _, ok := got[field]; !ok {
			t.Errorf("missing field %s", field)
		}
	}
	if len(got) != 9 {
		t.Errorf("got %d fields, want 9", len(got))
	}
	if got["type"] != "OrderCancelled" {
		t.Errorf("type = %v", got["type"])
	}
}

func TestNewRejectsUnknownStatus(t *testing.T) {
	if _, err := New(uuid.New(), 1, "REFUNDED", 1, "P1", "c"); err == nil {
		t.Fatal("want error")
	}
}
