package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"dualwrite-study/internal/events"
	"dualwrite-study/internal/patterns"
	"dualwrite-study/internal/store"
)

type fakePattern struct {
	got []patterns.OrderChange
	err error
}

func (f *fakePattern) Apply(_ context.Context, change patterns.OrderChange) error {
	f.got = append(f.got, change)
	return f.err
}

func post(t *testing.T, p patterns.Pattern, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	newMux(p, time.Second).ServeHTTP(rec, req)
	return rec
}

func TestCreateOrder(t *testing.T) {
	p := &fakePattern{}
	rec := post(t, p, "/orders", `{"amount": 499.5}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("code = %d", rec.Code)
	}
	var body struct {
		OrderID uuid.UUID `json:"order_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(p.got) != 1 || p.got[0].OrderID != body.OrderID || p.got[0].Amount != 499.5 || p.got[0].Status != "" {
		t.Fatalf("change = %+v, response id = %s", p.got, body.OrderID)
	}
}

func TestCreateOrderRejectsBadAmount(t *testing.T) {
	for _, body := range []string{`{}`, `{"amount": -1}`, `nonsense`} {
		p := &fakePattern{}
		if rec := post(t, p, "/orders", body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: code = %d", body, rec.Code)
		}
		if len(p.got) != 0 {
			t.Errorf("%s: pattern was called", body)
		}
	}
}

func TestTransitions(t *testing.T) {
	id := uuid.New()
	for path, status := range map[string]string{"pay": events.StatusPaid, "cancel": events.StatusCancelled} {
		p := &fakePattern{}
		rec := post(t, p, fmt.Sprintf("/orders/%s/%s", id, path), "")
		if rec.Code != http.StatusNoContent {
			t.Fatalf("%s: code = %d", path, rec.Code)
		}
		if p.got[0].OrderID != id || p.got[0].Status != status {
			t.Fatalf("%s: change = %+v", path, p.got[0])
		}
	}
}

func TestErrorMapping(t *testing.T) {
	id := uuid.New()
	if rec := post(t, &fakePattern{err: store.ErrNoTransition}, "/orders/"+id.String()+"/pay", ""); rec.Code != http.StatusConflict {
		t.Errorf("no transition: code = %d", rec.Code)
	}
	if rec := post(t, &fakePattern{err: errors.New("boom")}, "/orders/"+id.String()+"/pay", ""); rec.Code != http.StatusInternalServerError {
		t.Errorf("failure: code = %d", rec.Code)
	}
	if rec := post(t, &fakePattern{}, "/orders/not-a-uuid/pay", ""); rec.Code != http.StatusBadRequest {
		t.Errorf("bad id: code = %d", rec.Code)
	}
}
