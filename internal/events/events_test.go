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

func TestDecodeRoundTrip(t *testing.T) {
	want, err := New(uuid.New(), 2, StatusPaid, 10.5, "P1", "c")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.OrderID != want.OrderID || got.Seq != 2 || got.EventID != want.EventID {
		t.Fatalf("got %+v", got)
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	for _, value := range []string{`nonsense`, `{}`, `{"order_id":"6f1c7a3e-0f3b-4f7e-9a53-0c8f2f1f2f10"}`} {
		if _, err := Decode([]byte(value)); err == nil {
			t.Errorf("%s: want error", value)
		}
	}
}
