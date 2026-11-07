package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const connectorDir = "deploy/connect"

func isCDC(pattern string) bool { return pattern == "P4" || pattern == "P5" }

var slugUnsafe = regexp.MustCompile(`[^a-z0-9]+`)

// cycleSlug is the cycle ID in the characters a replication slot allows.
func cycleSlug(cycleID string) string {
	return slugUnsafe.ReplaceAllString(strings.ToLower(cycleID), "_")
}

// connectorConfig fills the pattern's template for the cycle. The connector name
// includes the run ID because Connect keeps offsets by name after a connector is
// deleted: a retried cycle would otherwise resume from a position that is gone.
func connectorConfig(c Cycle, runID string) ([]byte, error) {
	raw, err := os.ReadFile(fmt.Sprintf("%s/%s.json", connectorDir, strings.ToLower(c.Pattern)))
	if err != nil {
		return nil, fmt.Errorf("read connector template: %w", err)
	}
	filled := strings.NewReplacer(
		"{{cycle_id}}", c.ID,
		"{{run_id}}", runID,
		"{{cycle_slug}}", cycleSlug(c.ID),
		"{{acks}}", c.Env["KAFKA_ACKS"],
		"{{poll_ms}}", c.Env["DEBEZIUM_POLL_INTERVAL_MS"],
	).Replace(string(raw))
	return []byte(filled), nil
}

// createConnector starts the connector and waits until its slot is streaming,
// so no change made after this returns can be missed.
func (r *Runner) createConnector(ctx context.Context, c Cycle) error {
	body, err := connectorConfig(c, r.opts.RunID)
	if err != nil {
		return err
	}
	if _, err := r.connectRequest(ctx, http.MethodPost, "/connectors", body); err != nil {
		return fmt.Errorf("create connector: %w", err)
	}
	conn, err := pgx.Connect(ctx, r.measurement.PostgresDSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	for {
		var active bool
		err := conn.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name = $1`, "slot_"+cycleSlug(c.ID)).Scan(&active)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("read slot: %w", err)
		}
		if active {
			return nil
		}
		if err := sleep(ctx, 100*time.Millisecond); err != nil {
			return fmt.Errorf("slot never became active: %w", err)
		}
	}
}

// removeConnectors deletes every connector and logical slot. Leftover slots
// retain WAL and leak state between cycles (SPEC 9.3).
func (r *Runner) removeConnectors(ctx context.Context) error {
	raw, err := r.connectRequest(ctx, http.MethodGet, "/connectors", nil)
	if err != nil {
		return fmt.Errorf("list connectors: %w", err)
	}
	var names []string
	if err := json.Unmarshal(raw, &names); err != nil {
		return fmt.Errorf("decode connectors: %w", err)
	}
	for _, name := range names {
		if _, err := r.connectRequest(ctx, http.MethodDelete, "/connectors/"+name, nil); err != nil {
			return fmt.Errorf("delete connector %s: %w", name, err)
		}
	}
	conn, err := pgx.Connect(ctx, r.measurement.PostgresDSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	for {
		// A slot stays active for a moment after its connector is deleted.
		var remaining int
		err := conn.QueryRow(ctx, `SELECT count(pg_drop_replication_slot(slot_name)) FILTER (WHERE NOT active),
			count(*) FILTER (WHERE active)
			FROM pg_replication_slots WHERE slot_type = 'logical'`).Scan(new(int), &remaining)
		if err != nil {
			return fmt.Errorf("drop slots: %w", err)
		}
		if remaining == 0 {
			return nil
		}
		if err := sleep(ctx, 200*time.Millisecond); err != nil {
			return fmt.Errorf("slots still active: %w", err)
		}
	}
}

func (r *Runner) connectRequest(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, method, r.measurement.ConnectURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: %d: %s", method, path, response.StatusCode, payload)
	}
	return payload, nil
}

// sampleState records, once a second, the most WAL retained by the cycle's slot
// and the largest unsent outbox backlog. The returned function stops sampling and
// gives both maxima.
func (r *Runner) sampleState(ctx context.Context, c Cycle) func() (int64, int64) {
	ctx, cancel := context.WithCancel(ctx)
	type maxima struct{ wal, backlog int64 }
	result := make(chan maxima, 1)
	go func() {
		var highest maxima
		defer func() { result <- highest }()
		conn, err := pgx.Connect(ctx, r.measurement.PostgresDSN)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
		for sleep(ctx, time.Second) == nil {
			var value int64
			if isCDC(c.Pattern) {
				err := conn.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
					FROM pg_replication_slots WHERE slot_name = $1`, "slot_"+cycleSlug(c.ID)).Scan(&value)
				if err == nil {
					highest.wal = max(highest.wal, value)
				}
			}
			if c.Pattern == "P3" {
				err := conn.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&value)
				if err == nil {
					highest.backlog = max(highest.backlog, value)
				}
			}
		}
	}()
	return func() (int64, int64) {
		cancel()
		highest := <-result
		return highest.wal, highest.backlog
	}
}
