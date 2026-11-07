// Command orderservice accepts order changes over HTTP and applies them through
// the pattern selected by PATTERN (SPEC 6).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"dualwrite-study/internal/config"
	"dualwrite-study/internal/events"
	"dualwrite-study/internal/obs"
	"dualwrite-study/internal/patterns"
	"dualwrite-study/internal/publish"
	"dualwrite-study/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("orderservice failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	obs.Setup("orderservice", cfg.Pattern, cfg.CycleID)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	st, err := store.Open(ctx, cfg.PostgresDSN, cfg.PGMaxConns)
	if err != nil {
		return err
	}
	defer st.Close()

	pub, err := publish.New(cfg)
	if err != nil {
		return err
	}
	defer pub.Close()

	pattern, err := patterns.New(cfg, st, pub)
	if err != nil {
		return err
	}

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           newMux(pattern, cfg.HandlerTimeout),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	slog.Info("orderservice started", "addr", cfg.HTTPAddr, "topic", cfg.Topic())

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}

	// Graceful shutdown (SPEC 6.5): finish in-flight requests, then flush Kafka.
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := pub.Flush(shutdownCtx); err != nil {
		return err
	}
	slog.Info("shutdown complete")
	return nil
}

func newMux(pattern patterns.Pattern, timeout time.Duration) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Amount float64 `json:"amount"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Amount <= 0 {
			http.Error(w, "body must be {\"amount\": <positive number>}", http.StatusBadRequest)
			return
		}
		id := uuid.New()
		if !apply(w, r, pattern, timeout, patterns.OrderChange{OrderID: id, Amount: body.Amount}) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"order_id": id.String()})
	})
	transition := func(status string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			id, err := uuid.Parse(r.PathValue("id"))
			if err != nil {
				http.Error(w, "invalid order id", http.StatusBadRequest)
				return
			}
			if apply(w, r, pattern, timeout, patterns.OrderChange{OrderID: id, Status: status}) {
				w.WriteHeader(http.StatusNoContent)
			}
		}
	}
	mux.HandleFunc("POST /orders/{id}/pay", transition(events.StatusPaid))
	mux.HandleFunc("POST /orders/{id}/cancel", transition(events.StatusCancelled))
	return mux
}

// apply runs the change and writes the error response if it fails.
func apply(w http.ResponseWriter, r *http.Request, pattern patterns.Pattern, timeout time.Duration, change patterns.OrderChange) bool {
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	err := pattern.Apply(ctx, change)
	switch {
	case err == nil:
		return true
	case errors.Is(err, store.ErrNoTransition):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		slog.Error("apply failed", "order_id", change.OrderID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
	return false
}
