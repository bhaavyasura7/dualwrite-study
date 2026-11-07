package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"dualwrite-study/internal/checker"
)

const connectorDir = "deploy/connect"

func isCDC(pattern string) bool { return pattern == "P4" || pattern == "P5" }

var slugUnsafe = regexp.MustCompile(`[^a-z0-9]+`)

// cycleSlug is the cycle ID in the characters a replication slot allows.
func cycleSlug(cycleID string) string {
	return slugUnsafe.ReplaceAllString(strings.ToLower(cycleID), "_")
}

// connector is one connector's identity within a cycle.
type connector struct {
	suffix   string // added to the name and slot, to tell a post-failover connector apart
	snapshot string // overrides snapshot.mode if set
}

func slotName(c Cycle, k connector) string { return "slot_" + cycleSlug(c.ID) + k.suffix }

// connectorConfig fills the pattern's template for the cycle. The connector name
// includes the run ID because Connect keeps offsets by name after a connector is
// deleted: a retried cycle would otherwise resume from a position that is gone.
func connectorConfig(c Cycle, runID string, k connector) ([]byte, error) {
	raw, err := os.ReadFile(fmt.Sprintf("%s/%s.json", connectorDir, strings.ToLower(c.Pattern)))
	if err != nil {
		return nil, fmt.Errorf("read connector template: %w", err)
	}
	filled := strings.NewReplacer(
		"{{cycle_id}}", c.ID,
		"{{run_id}}", runID+k.suffix,
		"{{cycle_slug}}", cycleSlug(c.ID),
		"{{acks}}", c.Env["KAFKA_ACKS"],
		"{{poll_ms}}", c.Env["DEBEZIUM_POLL_INTERVAL_MS"],
		"{{retry_wait_ms}}", c.Env["DEBEZIUM_RETRY_WAIT_MS"],
	).Replace(string(raw))
	var doc struct {
		Name   string            `json:"name"`
		Config map[string]string `json:"config"`
	}
	if err := json.Unmarshal([]byte(filled), &doc); err != nil {
		return nil, fmt.Errorf("parse connector template: %w", err)
	}
	doc.Config["slot.name"] = slotName(c, k)
	if k.snapshot != "" {
		doc.Config["snapshot.mode"] = k.snapshot
	}
	if c.Env["CONNECT_EOS"] == "on" {
		doc.Config["exactly.once.support"] = "required"
	}
	if c.Env["KAFKA_IDEMPOTENT"] == "false" {
		doc.Config["producer.override.enable.idempotence"] = "false"
	}
	return json.Marshal(doc)
}

// createConnector starts the connector and waits until its slot is streaming,
// so no change made after this returns can be missed.
func (r *Runner) createConnector(ctx context.Context, c Cycle, k connector, dsn string) error {
	body, err := connectorConfig(c, r.opts.RunID, k)
	if err != nil {
		return err
	}
	if _, err := r.connectRequest(ctx, http.MethodPost, "/connectors", body); err != nil {
		return fmt.Errorf("create connector: %w", err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	for {
		var active bool
		err := conn.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slotName(c, k)).Scan(&active)
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

// deleteConnectors removes every connector through Connect's API.
func (r *Runner) deleteConnectors(ctx context.Context) error {
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
	return nil
}

// ensureWorker recreates Kafka Connect if the cycle needs a different worker
// configuration. Worker settings only take effect at creation (SPEC 9.2).
func (r *Runner) ensureWorker(ctx context.Context, c Cycle) error {
	support := "disabled"
	if c.Env["CONNECT_EOS"] == "on" {
		support = "enabled"
	}
	flush, err := r.docker.EnvValue(ctx, connectContainer, "OFFSET_FLUSH_INTERVAL_MS")
	if err != nil {
		return err
	}
	eos, err := r.docker.EnvValue(ctx, connectContainer, "CONNECT_EXACTLY_ONCE_SOURCE_SUPPORT")
	if err != nil {
		return err
	}
	if flush == c.Env["OFFSET_FLUSH_MS"] && eos == support {
		return nil
	}
	slog.Info("recreating kafka connect", "offset_flush_ms", c.Env["OFFSET_FLUSH_MS"], "eos", support)
	command := exec.CommandContext(ctx, "docker", "compose", "up", "-d", "--wait", "--force-recreate", "--no-deps", "kafka-connect")
	command.Env = append(composeEnv(c.Topology.Brokers), "OFFSET_FLUSH_MS="+c.Env["OFFSET_FLUSH_MS"], "CONNECT_EOS_WORKER="+support)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("recreate connect: %w: %s", err, output)
	}
	return nil
}

// removeConnectors deletes every connector and logical slot. Leftover slots
// retain WAL and leak state between cycles (SPEC 9.3).
func (r *Runner) removeConnectors(ctx context.Context) error {
	if err := r.deleteConnectors(ctx); err != nil {
		return err
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

// sampleState records, once a second, the most WAL retained by the cycle's slot,
// the largest unsent outbox backlog, and every logical slot's position. The
// returned function stops sampling and gives the results.
func (r *Runner) sampleState(ctx context.Context, c Cycle) func() (int64, int64, []checker.SlotSample) {
	ctx, cancel := context.WithCancel(ctx)
	type outcome struct {
		wal, backlog int64
		slots        []checker.SlotSample
	}
	result := make(chan outcome, 1)
	go func() {
		var out outcome
		defer func() { result <- out }()
		conn, err := pgx.Connect(ctx, r.measurement.PostgresDSN)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
		for sleep(ctx, time.Second) == nil {
			var value int64
			if isCDC(c.Pattern) {
				err := conn.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
					FROM pg_replication_slots WHERE slot_name = $1`, slotName(c, connector{})).Scan(&value)
				if err == nil {
					out.wal = max(out.wal, value)
				}
				out.slots = append(out.slots, readSlots(ctx, conn)...)
			}
			if c.Pattern == "P3" {
				err := conn.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&value)
				if err == nil {
					out.backlog = max(out.backlog, value)
				}
			}
		}
	}()
	return func() (int64, int64, []checker.SlotSample) {
		cancel()
		out := <-result
		return out.wal, out.backlog, out.slots
	}
}

func readSlots(ctx context.Context, conn *pgx.Conn) []checker.SlotSample {
	rows, err := conn.Query(ctx, `SELECT slot_name, active, COALESCE(confirmed_flush_lsn::text, ''),
		COALESCE(restart_lsn::text, ''), pg_current_wal_lsn()::text
		FROM pg_replication_slots WHERE slot_type = 'logical'`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var samples []checker.SlotSample
	now := time.Now()
	for rows.Next() {
		sample := checker.SlotSample{Time: now}
		if rows.Scan(&sample.Slot, &sample.Active, &sample.ConfirmedFlush, &sample.Restart, &sample.CurrentWAL) == nil {
			samples = append(samples, sample)
		}
	}
	return samples
}
