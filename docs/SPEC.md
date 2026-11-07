---
title: "Technical Specification: Fault-Injection Study of Database–Kafka Consistency Patterns"
subtitle: "Go implementation · version 1.8 · November 2026"
---

# 1. Purpose and scope

This document specifies everything needed to build and run the study: the system under test, the fault-injection harness, the checker, the experiment runner and the data it produces. It is written for the team members who will implement it. **Go is the primary language** for every component we write; the only exception is the final statistics and plotting, where Python is recommended (Section 15).

## 1.1 Goals

1. Implement five database-to-Kafka synchronisation patterns (P1–P5) in one Go service, selectable by configuration.
2. Inject six classes of failure, at random moments and at exact code points, fully automated.
3. Measure lost, ghost, duplicate and out-of-order events, the inconsistency window, business damage and performance cost.
4. Run the full test catalogue (~45 vectors, ~3,000 cycles) unattended and reproducibly on a laptop.
5. Keep enough per-event evidence to explain *why* every inconsistency happened.

## 1.2 Non-goals

- No production deployment, authentication, UI, or multi-region setup.
- No second database (e.g. MySQL) or second broker (e.g. RabbitMQ) in this version.
- No new synchronisation protocol; we measure existing patterns.

## 1.3 Pattern reference

| ID | Pattern | Who publishes to Kafka |
|---|---|---|
| P1 | Naive dual write | Order service, after commit |
| P2 | Dual write + in-memory retry | Order service, after commit, with retries |
| P3 | Transactional outbox, polling relay | Relay process (Go) |
| P4 | Transactional outbox via CDC | Debezium (Outbox Event Router) |
| P5 | Pure CDC on business table | Debezium |

# 2. Technology stack

Pin every version in `docker-compose.yml` and `go.mod`, and record them in every results file. Results are only comparable within one pinned stack.

| Area | Choice | Notes |
|---|---|---|
| Language | Go 1.23 or newer | Single module, `go vet` + `golangci-lint` clean |
| Postgres driver | `github.com/jackc/pgx/v5` (`pgxpool`) | Native protocol, explicit transactions |
| Kafka client | `github.com/twmb/franz-go` (`pkg/kgo`, `pkg/kadm`) | Pure Go (no cgo), supports idempotent producer, acks, linger, manual commits; admin client for topics and offsets |
| Docker control | `github.com/docker/docker/client` | Kill, pause, unpause, restart, stop containers |
| Network faults | Toxiproxy server + `github.com/Shopify/toxiproxy/v2/client` | Latency, withheld responses, connection resets and cuts |
| Config | `gopkg.in/yaml.v3` | Test catalogue and run configuration |
| Logging | `log/slog` (JSON handler) | Structured logs; required for root-cause analysis |
| UUIDs | `github.com/google/uuid` | Event and order IDs |
| Database | PostgreSQL 16 (official image), primary + streaming replica | `wal_level=logical` |
| Broker | Apache Kafka (official `apache/kafka` image), KRaft mode | 1 broker by default, 3 for broker-failure tests |
| CDC | Debezium Postgres connector on Kafka Connect (`quay.io/debezium/connect`) | Uses `pgoutput` |
| Proxy | Toxiproxy (`ghcr.io/shopify/toxiproxy`) | All app ↔ Kafka and app ↔ Postgres traffic goes through it |
| Analysis | Python 3 (pandas, matplotlib, scipy) | Only for statistics and plots |

> **Verify before freezing versions:** (1) the Kafka version's producer defaults; (2) franz-go's default linger and idempotence settings; (3) which Debezium version supports Kafka Connect exactly-once source mode for Postgres. Set all three explicitly in code/config rather than relying on defaults.

All images must have `arm64` builds (the team uses Apple Silicon Macs).

# 3. Repository layout

```
dualwrite-study/
├── go.mod
├── Makefile
├── docker-compose.yml
├── deploy/
│   ├── postgres/           # primary + replica init scripts, postgresql.conf overrides
│   ├── connect/            # Debezium connector JSON templates (P4, P5)
│   └── toxiproxy.json      # proxy definitions
├── cmd/
│   ├── orderservice/       # HTTP API + patterns P1–P5 (producer side)
│   ├── relay/              # outbox polling relay (P3)
│   ├── consumer/           # naive / dedup / upsert consumers (group G)
│   ├── loadgen/            # constant-rate and burst load generator
│   ├── checker/            # consistency checker (CLI)
│   └── runner/             # experiment runner (reads vectors.yaml)
├── internal/
│   ├── config/             # env + YAML config structs
│   ├── crash/              # targeted crash points
│   ├── events/             # event schema, (de)serialisation
│   ├── store/              # Postgres access: orders, outbox, processed_events
│   ├── publish/            # Kafka producer wrapper + logging markers
│   ├── patterns/           # P1..P5 implementations behind one interface
│   ├── faults/             # docker + toxiproxy + failover fault injectors
│   ├── checker/            # comparison logic (library, unit-tested)
│   ├── runner/             # cycle state machine, reset, drain, results writer
│   └── obs/                # slog setup, log markers, timing helpers
├── experiments/
│   └── vectors.yaml        # the test catalogue
├── results/                # results.csv, damage/, logs/ (git-ignored except CSV)
└── analysis/               # Python notebooks and plotting scripts
```

# 4. System architecture

```
 loadgen ──HTTP──▶ orderservice ──▶ toxiproxy:pg ──▶ postgres-primary ──▶ postgres-replica
                        │                                    │ (WAL)
                        │ (P1/P2)                            ▼
                        └──▶ toxiproxy:kafka ──▶ kafka ◀── kafka-connect + Debezium (P4/P5)
                                                   ▲  ▲
                              relay (P3) ──────────┘  └──── consumer(s) ──▶ postgres (side effects)

 runner ── docker API / toxiproxy API ──▶ any container        checker ──▶ postgres + kafka (read-only)
```

| Component | Responsibility |
|---|---|
| orderservice | Accepts orders and order changes over HTTP; writes to Postgres; publishes (P1/P2) or writes outbox rows (P3/P4); exposes crash points |
| relay | P3 only; publishes unsent outbox rows in batches; 1–4 instances |
| kafka-connect | P4/P5 only; Debezium streams changes from the WAL |
| consumer | Reads events, applies side effects using one of three strategies |
| loadgen | Generates order traffic at a fixed rate or in bursts |
| runner | Executes the test catalogue: reset, start, load, fault, recover, drain, check, record |
| checker | Compares Postgres with Kafka (and consumer side effects) and reports damage |
| toxiproxy | Sits between the app and Postgres/Kafka; used for network faults and DB failover redirection |

**Rule:** every component, including Kafka Connect, connects to Postgres and Kafka **through Toxiproxy**, never directly. Application clients and Kafka Connect use **separate proxy groups** (`pg-app`/`pg-connect`, `kafka-app-bN`/`kafka-connect-bN`), so a fault can target the application side, the CDC side, or both (Section 16). This is what makes network faults and failover redirection possible without code changes.

# 5. Data model

## 5.1 Postgres schema

```sql
CREATE TABLE orders (
  order_id    UUID PRIMARY KEY,
  status      TEXT        NOT NULL,          -- PLACED | PAID | CANCELLED
  version     INT         NOT NULL,          -- +1 on every change; = event seq
  amount      NUMERIC(12,2) NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Column names follow Debezium Outbox Event Router defaults
CREATE TABLE outbox (
  id             UUID PRIMARY KEY,           -- event_id
  aggregatetype  TEXT        NOT NULL,       -- 'order' (routes to topic)
  aggregateid    TEXT        NOT NULL,       -- order_id (Kafka key)
  type           TEXT        NOT NULL,       -- OrderPlaced | OrderPaid | OrderCancelled
  seq            INT         NOT NULL,       -- = orders.version
  payload        JSONB       NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at        TIMESTAMPTZ                 -- NULL = unsent (P3 relay only)
);
CREATE INDEX outbox_unsent ON outbox (created_at) WHERE sent_at IS NULL;

-- Every committed version, for the checker (Section 11.2)
CREATE TABLE orders_history (
  order_id  UUID NOT NULL,
  version   INT  NOT NULL,
  status    TEXT NOT NULL,
  PRIMARY KEY (order_id, version)
);
CREATE FUNCTION record_order_version() RETURNS trigger AS $$
BEGIN
  INSERT INTO orders_history (order_id, version, status)
  VALUES (NEW.order_id, NEW.version, NEW.status);
  RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER orders_history_trg AFTER INSERT OR UPDATE ON orders
  FOR EACH ROW EXECUTE FUNCTION record_order_version();

-- Consumer side (group G): one schema, one table set PER STRATEGY.
-- All three strategies consume the same stream in parallel and never share tables.
CREATE SCHEMA consumer;
CREATE TABLE consumer.payments_naive (
  payment_id BIGSERIAL PRIMARY KEY,
  order_id UUID NOT NULL, event_key TEXT NOT NULL,
  amount NUMERIC(12,2) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);                                                 -- no unique constraint: duplicates must be visible
CREATE TABLE consumer.payments_dedup (LIKE consumer.payments_naive INCLUDING ALL);
CREATE TABLE consumer.processed_events (event_key TEXT PRIMARY KEY);   -- dedup strategy
CREATE TABLE consumer.payments_upsert (LIKE consumer.payments_naive INCLUDING ALL);
CREATE UNIQUE INDEX payments_upsert_order ON consumer.payments_upsert (order_id);
```

`event_key` is the `event_id` for P1–P4 and `"<order_id>:<seq>"` for P5, which has no event ID (Section 10). The commit time of each version is read later with `pg_xact_commit_timestamp(xmin)` on `orders_history` (needs `track_commit_timestamp = on`, Section 9.1).

## 5.2 Event schema

Every event, from every pattern, has the same JSON value. The checker relies on these fields.

```json
{
  "event_id":   "uuid",
  "order_id":   "uuid",
  "seq":        3,
  "type":       "OrderCancelled",
  "amount":     499.00,
  "status":     "CANCELLED",
  "created_at": "2026-11-20T10:15:30.123Z",
  "pattern":    "P3",
  "cycle_id":   "B-1_P3_r07"
}
```

- **Kafka key:** `order_id` (keeps one order's events in one partition).
- **P5 exception:** Debezium emits row-change envelopes, not this schema. The checker has a P5 adapter that maps `after.order_id`, `after.version` (as `seq`) and `after.status` into the same internal form. P5 has no `event_id`; duplicates are detected by `(order_id, seq)`.

## 5.3 Topics

- One **fresh topic per cycle**: `orders.<cycle_id>` (e.g. `orders.A-4_P3_r07`), 3 partitions, replication factor 1 (3 for broker tests).
- Every cycle topic sets `message.timestamp.type=LogAppendTime`. The window metric needs the broker's clock; with the default `CreateTime`, a record delayed by retries keeps its early timestamp and the window is understated. The checker refuses any other topic.
- Creating new topics avoids Kafka's slow, flaky topic deletion. Old topics are deleted in bulk at the end of a run.
- For P4 the Outbox Event Router routes to the topic via a per-cycle `route.topic.replacement`; for P5, Debezium names topics `<prefix>.<schema>.<table>`, so a `RegexRouter` transform renames `.*\.public\.orders` to `orders.<cycle_id>` (Section 9.3).

# 6. Order service (`cmd/orderservice`)

## 6.1 HTTP API

| Method and path | Effect |
|---|---|
| `POST /orders` | Create order (status PLACED, version 1); body `{amount}`; returns `order_id` |
| `POST /orders/{id}/pay` | Status → PAID, version + 1 |
| `POST /orders/{id}/cancel` | Status → CANCELLED, version + 1 |
| `GET /healthz` | Liveness for the runner |

Multiple changes per order (place → pay or place → cancel) are required so that sequence numbers and ordering can be checked.

## 6.2 Pattern interface

```go
type Pattern interface {
    // Apply performs one business change and whatever publishing the pattern requires.
    Apply(ctx context.Context, change OrderChange) error
}
```

Selected at startup by `PATTERN=P1..P5`. Implementations live in `internal/patterns`.

## 6.3 Behaviour per pattern

**P1: naive dual write**
```go
tx := begin()
upsertOrder(tx, change)                 // version++
crash.Point("before_db_commit")         // A-2
tx.Commit()
crash.Point("after_db_commit")          // A-1, A-1b
publisher.Send(ctx, event)              // async produce; logs SENT_CALLED
crash.Point("after_send_before_flush")  // A-8
```

**P2: dual write + retry.** As P1, but on publish failure: retry up to `RETRY_MAX` times with exponential back-off (`RETRY_BASE_MS`, doubling, capped) held **in memory** only. Crash point `during_retry_backoff` (A-3) fires at the start of the first back-off sleep.

**P3/P4: outbox.**
```go
tx := begin()
upsertOrder(tx, change)
insertOutbox(tx, event)                 // same transaction
crash.Point("before_db_commit")
tx.Commit()
crash.Point("after_db_commit")          // A-1b: expect no damage
// no publishing here; relay (P3) or Debezium (P4) publishes
```

**P5: pure CDC.** Only `upsertOrder` in a transaction; no outbox, no publishing.

## 6.4 Publisher (`internal/publish`)

A thin wrapper around `kgo.Client` with settings taken from config, never defaults:

| Setting | Config key | Used by |
|---|---|---|
| Required acks (1 / all) | `KAFKA_ACKS` | C-4, I-4 |
| Idempotent writes on/off | `KAFKA_IDEMPOTENT` | F-3 |
| Linger (ms) | `KAFKA_LINGER_MS` | I-5 |
| Sync vs async send | `KAFKA_SYNC_SEND` | A-8 control |
| Request timeout | `KAFKA_REQUEST_TIMEOUT_MS` (`kgo.ProduceRequestTimeout`) | F-1, F-2 |
| Delivery timeout | `KAFKA_DELIVERY_TIMEOUT_MS` (`kgo.RecordDeliveryTimeout`) | A-3, B-3, C-1 |
| Request timeout overhead | `KAFKA_REQUEST_OVERHEAD_MS` (`kgo.RequestTimeoutOverhead`, default 1000 here; library default 10 s) | F-2, F-3 |
| Record buffer | `KAFKA_MAX_BUFFERED_RECORDS` (`kgo.MaxBufferedRecords`, default 50000) | C-1 |

It logs three markers per event (Section 13): `SENT_CALLED` when `Produce` is called, `SEND_ACKED` in the promise callback on success, `SEND_FAILED` on error. These markers distinguish loss before sending from loss in the producer buffer.

**Client-internal retries:** franz-go retries failed produce requests itself. A record only fails back to the application once `KAFKA_DELIVERY_TIMEOUT_MS` expires, so P2's application-level retries start only after that. Always set the delivery timeout explicitly; with the library default the P2 retry path may never run.

**Buffer limit (decided).** The buffer stays at 50,000. C-1's 300 s outage needs about 53,400 records at 178 events/s, so `Produce` blocks and the backpressure reaches the application. This is deliberate: the blocking is part of the result. Every row records the value in `env_json`.

**franz-go behaviours the paper must report** (from the library source; to be confirmed under real faults):

- *Hung connections are detected late.* The client waits request timeout + overhead before giving up on a connection; with the library's 10 s overhead a 5 s withheld response would go unnoticed. Hence `KAFKA_REQUEST_OVERHEAD_MS = 1000`.
- *Idempotent records in flight cannot time out.* With idempotence on, a record already sent and awaiting a response is not failed by the delivery timeout (failing it would break the sequence numbers). B-3 therefore sweeps `KAFKA_IDEMPOTENT`.
- *acks=1 requires idempotence off.* C-4 runs every cell with idempotence off so that only `acks` differs.
- *One timeout fails the partition's whole buffer.* When one record times out, franz-go fails every record buffered for that partition, to preserve ordering. P1/P2 losses under broker outages are therefore batch-sized; the checker counts `SEND_FAILED` markers per cycle (`send_failed`) so the paper can show it.

## 6.5 Graceful shutdown

On SIGTERM every component must shut down cleanly, or control C0-3 fails: the order service stops accepting requests, finishes in-flight ones and calls `Flush` on the Kafka client before exiting; the relay finishes its current batch (publish and mark) before exiting; consumers finish the current event and commit. SIGKILL (crash points and `crash` faults) bypasses all of this by design.

# 7. Targeted crash points (`internal/crash`)

```go
// Point kills the process immediately if CRASH_AT equals name.
// attrs identify the event(s) being processed at this line (event_id, order_id, seq, batch_id).
func Point(name string, attrs ...slog.Attr) {
    if name == "" || name != os.Getenv("CRASH_AT") { return }
    if !reachedN(name) { return }                    // see CRASH_AFTER_N
    slog.LogAttrs(ctx, slog.LevelInfo, "CRASH_POINT_HIT", append(attrs, slog.String("point", name))...)
    flushLogs()                                      // logs only; nothing else
    syscall.Kill(os.Getpid(), syscall.SIGKILL)
}
```

Requirements:

- **No cleanup.** The process must die like `kill -9`: no deferred functions, no Kafka flush, no transaction rollback by our code.
- **Containers must run with `init: true`** in Compose (tini as PID 1). The Linux kernel ignores SIGKILL sent from inside a PID namespace to its own PID 1, so without an init process the self-kill silently does nothing.
- `CRASH_AFTER_N` (default 50): the hook fires on the Nth time the point is reached **after arming**. With `ARM_ON_SIGNAL=true`, points stay inactive until the runner sends SIGUSR1 at the end of warm-up, so the system is in steady state when it dies. Without arming, 50 hits at about 178 changes/s fire 0.35 s into load, inside the warm-up.
- **Randomised firing point.** For each cycle the runner draws N uniformly from `[after_n, 2 × after_n]` using the cycle seed and records it (`crash_after_n`). A fixed point can line up with batching or flush cycles.
- **In-flight at crash.** The checker derives `inflight_at_crash` and `oldest_pending_ms` from the log markers, so collateral loss can be reported against what was in flight rather than as a mean with wide error bars.
- The runner confirms the crash by finding `CRASH_POINT_HIT` in the container log; a cycle where it is missing is marked `INVALID_NO_CRASH`.
- **Hooked vs collateral damage.** A crash point kills the whole process, so every other request in flight dies at a random point too. The marker therefore logs the event(s) at the hook, and the checker reports damage to those events (`hooked_*`) separately from damage to all others (`collateral_*`). Targeted asserts with `assert_scope: hooked` apply only to the former.
- **Changing `CRASH_AT` means recreating the container.** Docker fixes a container's environment at creation, so `ContainerStart` would bring the service back with the crash point still armed. The runner always recreates (remove + create) components whose environment changes, including after every crash.

## 7.1 Crash point catalogue

| Name | Component | Patterns | Vector |
|---|---|---|---|
| `before_db_commit` | orderservice | P1–P5 | A-2 |
| `after_db_commit` | orderservice | P1, P2 (P3–P5 as A-1b control) | A-1, A-1b |
| `during_retry_backoff` | orderservice | P2 | A-3 |
| `after_send_before_flush` | orderservice | P1 | A-8 |
| `after_read_before_publish` | relay | P3 | A-5 |
| `after_publish_before_mark` | relay | P3 | A-4 |
| `after_side_effect_before_commit` | consumer | all | G-1 |
| `after_commit_before_side_effect` | consumer | all | G-2 |

## 7.2 Stall points

`crash.Stall(name, attrs...)` works like `Point` but, instead of killing the process, sleeps for `STALL_MS` and logs `STALL_POINT_HIT` before and `STALL_POINT_END` after. Configured with `STALL_AT`, `STALL_MS`, `STALL_AFTER_N`. Stall points give the pause vectors that need exact timing (B-1, B-2, D-3) a deterministic trigger; random pauses (B-3 to B-5, G-4) still use `docker pause`. Difference to report in the paper: a stall freezes one goroutine, while the client libraries' background goroutines keep running.

| Name | Component | Vectors |
|---|---|---|
| `after_read_before_publish` | relay | B-1, B-2 |
| `after_publish_before_mark` | relay | D-3 |

# 8. Outbox relay (`cmd/relay`, P3)

```go
for {
    tx := begin()
    rows := tx.Query(`SELECT id, aggregateid, type, seq, payload FROM outbox
                      WHERE sent_at IS NULL ORDER BY created_at
                      LIMIT $1 FOR UPDATE SKIP LOCKED`, batchSize)
    crash.Point("after_read_before_publish")
    publishAll(rows)                       // wait for acks of the whole batch
    crash.Point("after_publish_before_mark")
    tx.Exec(`UPDATE outbox SET sent_at = now() WHERE id = ANY($1)`, ids)
    tx.Commit()
    sleepIfEmpty(pollInterval)
}
```

| Config | Default | Vectors |
|---|---|---|
| `RELAY_BATCH_SIZE` | 100 | A-7, I-2 |
| `RELAY_POLL_INTERVAL_MS` | 500 | I-1 |
| Number of relay replicas | 1 (2/4 for ordering tests) | B-1, B-5, H-3 |

The relay holds its transaction open while publishing so the row locks persist. This is intentional: it is what makes B-2 (lock released by Postgres's `idle_in_transaction_session_timeout` during a pause) possible.

# 9. CDC configuration (P4, P5)

## 9.1 Postgres settings

```
wal_level = logical
max_replication_slots = 20
max_wal_senders = 20
idle_in_transaction_session_timeout = 10s     # B-2 depends on this; record value
track_commit_timestamp = on                   # true commit times for the window metric; set on primary AND replica
```

## 9.2 Kafka Connect worker

```
offset.flush.interval.ms = 60000           # swept in E-2 / I-3 (1000, 10000, 60000)
exactly.once.source.support = disabled     # see CONNECT_EOS mapping below
connector.client.config.override.policy = All   # lets a connector override producer acks (C-4)
```

`CONNECT_EOS` maps to two different settings with different allowed values:

| `CONNECT_EOS` | Worker `exactly.once.source.support` | Connector `exactly.once.support` |
|---|---|---|
| off | `disabled` | omitted |
| on | `enabled` | `required` |

Worker settings only take effect when Kafka Connect is recreated, so the runner groups cycles by worker configuration and recreates Connect between groups. When EOS is on, Connect writes in transactions, so **every Kafka reader in the study (checker and consumers) uses `isolation.level=read_committed`**; otherwise aborted records would be counted as duplicates.

```
```

## 9.3 P4 connector (template, values substituted per cycle)

```json
{
  "name": "p4-{{cycle_id}}",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "toxiproxy", "database.port": "15433",
    "database.user": "app", "database.password": "app", "database.dbname": "shop",
    "slot.name": "slot_{{cycle_slug}}",
    "publication.name": "pub_outbox",
    "topic.prefix": "cdc{{cycle_slug}}",
    "table.include.list": "public.outbox",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.route.topic.replacement": "orders.{{cycle_id}}",
    "transforms.outbox.table.field.event.key": "aggregateid",
    "transforms.outbox.table.fields.additional.placement": "seq:header",
    "producer.override.acks": "{{acks}}"
  }
}
```

`exactly.once.support: "required"` is added only when `CONNECT_EOS=on`.

`poll.interval.ms` is set explicitly from `DEBEZIUM_POLL_INTERVAL_MS` (default 500) and recorded in `env_json`.

The templates in `deploy/connect/` also set what the study needs but this listing omits: `publication.autocreate.mode=disabled`, `snapshot.mode=no_data` (the cycle's tables are empty, and Debezium 3 replaced `never` with `no_data`), and converters. P4 uses `StringConverter` so the payload reaches Kafka as the common event schema. P5 uses `JsonConverter` without schemas and `decimal.handling.mode=double`.

P5 uses the same base with `table.include.list = public.orders`, `topic.prefix = cdc{{cycle_slug}}`, and a `RegexRouter` instead of the outbox transform:

```json
"transforms": "route",
"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.route.regex": ".*\\.public\\.orders",
"transforms.route.replacement": "orders.{{cycle_id}}"
```

**Failover recovery (`CDC_FAILOVER_RECOVERY`, D-1, D-2, H-2):** after promotion the slot does not exist on the new primary (PG16). The runner deletes the connector and recreates it against the new primary with a new slot and either `snapshot.mode=never` (`new_slot_no_snapshot`) or `snapshot.mode=initial` (`new_slot_initial_snapshot`).

**Connector names include the run ID.** Connect keeps offsets by connector name after the connector is deleted, so a retried cycle would otherwise resume from a position that no longer exists.

**Per-cycle hygiene:** create the connector at cycle start; at cycle end delete the connector **and** drop its replication slot (`SELECT pg_drop_replication_slot(...)`). Leftover slots retain WAL and leak state between cycles.

# 10. Consumers (`cmd/consumer`, group G)

One binary, `CONSUMER_STRATEGY = naive | dedup | upsert`. In group G cycles all three run **in parallel**, each in its own container, its own consumer group (`g-<strategy>-<cycle_id>`) and its own tables, so they see the identical event stream. Manual offset commits (auto-commit off), `isolation.level=read_committed`. `consumers_per_group` (default 1, 2 for G-4) sets the replicas per strategy.

| Strategy | Processing of one payment-triggering event |
|---|---|
| naive | Insert into `consumer.payments_naive`; commit offset |
| dedup | One DB transaction: insert `event_key` into `consumer.processed_events` (skip the event if it exists) and insert into `consumer.payments_dedup`; commit; then commit offset |
| upsert | `INSERT ... ON CONFLICT (order_id) DO NOTHING` into `consumer.payments_upsert`; commit offset |

**Input adapter.** Consumers share the checker's event adapter (`internal/events`): P1–P4 events are read as-is; P5 Debezium envelopes are mapped from `after.*`. A payment is triggered by an `OrderPaid` event (P1–P4) or a P5 row change whose `after.status` is `PAID`. `event_key` is `event_id`, or `"<order_id>:<seq>"` for P5.

`CONSUMER_COMMIT_ORDER = effect_first | offset_first` selects G-1 vs G-2 behaviour. Business damage is computed per strategy (Section 11).

# 11. Checker (`cmd/checker`, `internal/checker`)

## 11.1 Inputs

- Postgres: all rows of `orders` (final state and version) and the full `outbox` (for P3/P4 provenance).
- Kafka: every record of `orders.<cycle_id>` from offset 0 on all partitions, with partition, offset and broker timestamp, read with `isolation.level=read_committed`.
- Logs: the cycle's `SENT_CALLED` / `SEND_ACKED` markers (for loss attribution).
- Consumer schema: the three payment tables (group G only).
- The `CRASH_POINT_HIT` marker (hooked events).

**Source of truth after failover:** in D-vectors the database the checker reads is the **promoted replica**. Changes committed on the old primary but never replicated do not exist in the truth, so events for them count as ghosts.

## 11.2 Definitions

Let **DB** be the set of committed `(order_id, seq)` pairs (every version from 1 to the final `version` of each order) and **K** the multiset of `(order_id, seq)` read from Kafka.

| Metric | Definition |
|---|---|
| lost | pairs in DB and not in K |
| ghost | pairs in K and not in DB |
| duplicates | sum over pairs of (count in K − 1) where count > 1 |
| out_of_order | within each order (one partition), **first deliveries** whose `seq` is lower than a previously seen `seq` for that order. Repeat deliveries are excluded, so the damage types stay orthogonal |
| stale_redelivery | repeat deliveries whose `seq` is lower than a previously seen `seq` for that order. A subset of duplicates, not additive with it. It measures downstream impact: a last-write-wins consumer regresses on such a record although nothing was reordered |
| window (p50/p99/max) | broker timestamp of first occurrence − commit time of that version, `pg_xact_commit_timestamp(xmin)` of its `orders_history` row (not `now()`, which is the transaction start) |
| recovery_s | time from fault end until no lost pairs remain (sampled every second during drain) |
| lost_after_send | lost pairs whose event has a `SENT_CALLED` marker (producer-buffer loss) |
| business_damage_<strategy> | absolute difference between rows in that strategy's payment table and orders whose final status is PAID |
| hooked_lost / hooked_dup | damage to the event(s) named in `CRASH_POINT_HIT` |
| collateral_lost / collateral_dup | the same metrics for every other event |
| inflight_at_crash / oldest_pending_ms | records with `SENT_CALLED` but no outcome at `CRASH_POINT_HIT`, and the age of the oldest |
| relay_batches / batch_rows_mean / batch_rows_max | number of relay batches and the rows they actually held, from `RELAY_BATCH` markers (`phase=read`). The effective batch can be far below `RELAY_BATCH_SIZE` |
| outbox_backlog_max | maximum unsent outbox rows, sampled every second (P3), so a relay falling behind is measured rather than inferred from the window |
| wal_retained_bytes_max | maximum `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)` of the cycle's slot, sampled every second (P4/P5) |

**Version history:** DB is built from `orders_history` (Section 5.1), which a trigger fills on every insert/update of `orders`. The trigger runs in all patterns, so its overhead is equal across them.

## 11.3 Outputs

- One result struct appended to `results/results.csv` by the runner.
- `results/damage/<cycle_id>.json`: the exact `(order_id, seq)` pairs for every damage class, used for root-cause analysis.

## 11.4 Correctness requirements (control C0-2)

Golden tests in `internal/checker/testdata/`: hand-written DB and Kafka fixtures with known damage. `go test ./internal/checker/...` must pass before any experiment run, and runs in CI on every change. Required fixtures: no damage; 2 lost + 2 duplicates + 1 out-of-order + 1 ghost; a triple duplicate (counts as 2); 100,000 events with 50 planted problems.

# 12. Fault injection (`internal/faults`)

```go
type Fault interface {
    Inject(ctx context.Context) error   // start the fault
    Recover(ctx context.Context) error  // end it and restore service
    Name() string
}
```

| Fault | Implementation |
|---|---|
| `crash` | `ContainerKill(id, "SIGKILL")`; recovered by **recreating** the container with crash/stall settings cleared |
| `graceful_stop` | `ContainerStop` (SIGTERM, default grace period); `ContainerStart` after `duration_s` |
| `restart` | `ContainerRestart` |
| `pause` | `ContainerPause`; `ContainerUnpause` after `duration_s` |
| `broker_stop` | `ContainerStop` / `ContainerStart` on the broker container(s) |
| `proxy_disable` | `Disable` on every proxy in the target group; `Enable` on recover |
| `latency` | Toxiproxy `latency` toxic (`latency`, `jitter`) on both streams of the group |
| `withhold_downstream` | Toxiproxy `timeout` toxic with `timeout=0` on the **downstream** stream only: requests reach the server, responses never return (lost acknowledgements, F-2/F-3) |
| `reset_peer` | Toxiproxy `reset_peer` toxic with `toxicity` = share of connections reset. Toxiproxy works on TCP streams and cannot drop packets; true packet loss would need `tc netem` and is out of scope for v1 |
| `failover` | Kill primary → `SELECT pg_promote()` on the replica → repoint the `pg-app` and `pg-connect` proxies to the replica → apply `CDC_FAILOVER_RECOVERY` (Section 9.3). Applications reconnect through the same address |
| `kill_leader` | Look up the leader of partition 0 with `kadm`, SIGKILL that broker |

**After every failover cycle** (`requires_rebuild: true`) the runner rebuilds the original setup: recreate the primary from scratch (init scripts), re-seed the replica with `pg_basebackup`, and point both pg proxies back at the primary. The runner schedules all `requires_rebuild` vectors at the end of a run.

**Failover note:** with PostgreSQL 16, logical replication slots are not copied to the replica, so Debezium cannot simply resume after failover (vector D-2). PostgreSQL 17 can synchronise failover slots; testing PG16 vs PG17 is an optional extension.

# 13. Observability and evidence

All components log JSON via `slog` to stdout, with `cycle_id`, `component`, `pattern` on every line. Required markers:

| Marker | Emitted by | Fields |
|---|---|---|
| `CRASH_POINT_HIT` | any | point |
| `STALL_POINT_HIT` / `STALL_POINT_END` | relay | point, batch_id |
| `SENT_CALLED` | publisher | event_id, order_id, seq |
| `SEND_ACKED` | publisher | event_id, partition, offset |
| `SEND_FAILED` | publisher | event_id, error |
| `RETRY_SCHEDULED` | P2 | event_id, attempt, backoff_ms |
| `RELAY_BATCH` | relay | batch_id, n, first_id, last_id, phase (read/published/marked) |
| `CONSUMED` | consumer | event_id, partition, offset, action (applied/skipped) |

The runner collects container logs for each cycle; logs are **kept only for cycles with damage or errors** (gzipped), otherwise discarded.

# 14. Experiment runner (`cmd/runner`)

## 14.1 Test catalogue (`experiments/vectors.yaml`)

The full catalogue (controls, groups A–I and performance runs) is delivered as `vectors.yaml`, whose header documents its schema: modes, `sweep` and `cells`, `crash` and `stall` points, ordered `faults` with `at_s` or `on_log` triggers, `by_pattern` overrides, enforced `assert`s for controls, `requires_rebuild`, and the recovery rule. The runner must reject unknown keys, so a typo never silently changes an experiment.

Each `(vector, pattern, sweep combination, repeat)` is one **cycle**.

## 14.2 Cycle state machine

```
RESET → START → WARMUP → FAULT → RECOVER → DRAIN → CHECK → RECORD → (next)
```

| State | Action | Timeout |
|---|---|---|
| RESET | In this order: (1) stop load and app components; (2) delete the previous connector and drop its replication slot; (3) truncate all tables (incl. `orders_history`, consumer schema); (4) create topic `orders.<cycle_id>`; (5) reset Toxiproxy (remove toxics, enable proxies). Deleting the connector before truncating keeps a live connector from streaming the truncate | 60 s |
| START | **Recreate** components with the cycle's env (PATTERN, CRASH_AT/STALL_AT, sweep values, topology); recreate Kafka Connect if its worker config changed; create the connector (P4/P5); wait for `/healthz` | 90 s |
| WARMUP | Start loadgen; wait `warmup_s` | — |
| FAULT | Targeted: wait for `CRASH_POINT_HIT` / `STALL_POINT_HIT`. Random: inject each fault at uniform(`at_s`) or on its `on_log` marker | 60 s after the last scheduled fault |
| RECOVER | `Recover()` each fault; recreate crashed components with crash/stall settings cleared (recovery rule, 14.1) | 90 s |
| DRAIN | Stop load; wait until Kafka end offsets and outbox unsent count are unchanged for `quiet_s` | `timeout_s` |
| CHECK | Run checker library in-process | 120 s |
| RECORD | Append CSV row; write damage JSON; keep logs if damage > 0 or status ≠ OK | — |
| REBUILD | Only for `requires_rebuild` vectors (Section 12) | 180 s |

**Pre-cycle health check.** During the last 5 s of warm-up, before any fault is armed, the runner samples the Mac's load average, Postgres CPU and p99 latency, and records them as `pre_host_load`, `pre_db_cpu_pct` and `pre_p99_ms`. A cycle above `PRECHECK_MAX_P99_MS` (default 30) or `PRECHECK_MAX_HOST_LOAD` (default 8) gets `pre_flag` set to `latency`, `load` or both. It flags and never stops, so a spike can be traced to the machine or to the system.

**Preflight.** The run refuses to start if Connect's internal topics are missing or the disk has under 20 GB free.

Any timeout marks the cycle `FAILED_<STATE>`, saves its logs and moves on; one bad cycle never stops the run. A violated control `assert` marks `CONTROL_FAIL` and **does** stop the run. The runner is resumable: it skips cycles already present in `results.csv`.

**Sharding.** `--shard i/n` runs only the vectors whose `hash(vector_id) mod n = i-1`, so n laptops split the catalogue with no overlap and every cycle of a vector runs on one machine, and each writes `results/results.<host>.csv`. All laptops must use the same git commit and image digests (checked at start and recorded in every row). Performance vectors (`group: perf`) are never sharded: they run on one designated machine.

**Run-time budget.** Measured in Milestone 5 on P1 across A-1, A-2 and A-8: about 12 s of fixed overhead per cycle (reset, start, drain, check), of which 10 s is the drain's quiet period. At that figure the catalogue needs 1,790 cycles / ~41 h for `must`, 1,390 / ~28 h for `should`, and 420 / ~7 h for `optional`, about 77 h in total. P4 and P5 add connector creation and failover vectors add a rebuild, so measure those in Milestones 6 and 7. Measure the real overhead on 2–3 vectors in Milestone 5 and re-plan before committing to the full run, including whether C0-1 needs its full five hours. Correctness runs can be split across team laptops with identical pinned stacks; performance runs (P-1) stay on one machine.

## 14.3 Results CSV (one row per cycle)

```
run_id, cycle_id, vector_id, pattern, mode, fault, crash_point, repeat, seed,
status, sweep_json, env_json, versions_json, load_rate, orders_total, events_total,
lost, ghost, duplicates, out_of_order, stale_redelivery, lost_after_send,
hooked_lost, hooked_dup, collateral_lost, collateral_dup,
business_damage_naive, business_damage_dedup, business_damage_upsert,
wal_retained_bytes_max, outbox_backlog_max, relay_batches, batch_rows_mean, batch_rows_max,
send_failed, crash_after_n, inflight_at_crash, oldest_pending_ms,
host, shard, git_commit,
window_p50_ms, window_p99_ms, window_max_ms, recovery_s,
throughput_ops, latency_p50_ms, latency_p99_ms, db_cpu_pct, load_s,
pre_host_load, pre_db_cpu_pct, pre_p99_ms, pre_flag, started_at, finished_at
```

Every row carries the random seed and the pinned versions so any cycle can be replayed.

## 14.4 Commands

```
make up                    # docker compose up the base stack
make test                  # unit tests incl. checker golden tests (C0-2)
make run VECTORS=A-1,A-4   # run selected vectors
make run-all               # full catalogue (multi-day; use caffeinate -i on macOS)
make run-all SHARD=2/4     # this laptop's share of the catalogue (see below)
make merge                 # combine results/*.csv from all laptops into results.csv
make report                # run analysis scripts → figures/
```

# 15. Load generator (`cmd/loadgen`) and analysis

**Load generator:** open-loop (fixed schedule, independent of response time) so a slow system does not reduce offered load. Modes: `constant` (rate/s), `burst` (base rate with a 10× spike for N seconds). Each created order is later paid (60%) or cancelled (20%) after a random 50–500 ms, so every order has one or two versions in total (80% get one follow-up change, 20% none). It records per-request latency into an HDR-style histogram and reports p50/p99 and achieved throughput to the runner.

**Analysis:** Python scripts in `analysis/` read `results.csv` and produce, at minimum:
damage by pattern × fault (bars with 95% confidence intervals), a pattern × fault heatmap, knob sweeps (lines), exactly-once on/off and consumer-strategy comparisons, and inconsistency-window distributions.

**Analysis rule, fixed before the final runs.** Every result is reported twice: over all cycles, and excluding cycles with a `pre_flag`. "All cycles" is the primary number. Flagged cycles are never dropped from the data. The rule is set in advance because deciding after seeing the data would bias the results.

# 16. Docker Compose requirements

- All services `init: true` (crash hooks, Section 7).
- Memory limits: Postgres 1 GB each, Kafka 1.5 GB per broker, Connect 1.5 GB, Go services 256 MB each. About 7 GB with one broker and about 9 GB with three (C-4). **Set Docker Desktop's memory to 12 GB** before the three-broker vectors.
- Postgres replica bootstrapped with `pg_basebackup` in its entrypoint; streaming replication, asynchronous by default (synchronous for D-4).
- Toxiproxy proxies, grouped by client class so faults can target one side:

| Group | Proxy (listen → upstream) | Used by |
|---|---|---|
| `pg-app` | 15432 → primary:5432 | order service, relay, consumers |
| `pg-connect` | 15433 → primary:5432 | Kafka Connect |
| `kafka-app` | 1909N → broker-N:9092 (`APP` listener), one per broker | order service, relay, consumers |
| `kafka-connect` | 1919N → broker-N:9094 (`CONNECT` listener), one per broker | Kafka Connect |

- Clients connect to whatever address each broker **advertises**, so a single proxy cannot front three brokers. Each client class needs its **own broker listener and port**: `APP` on 9092 advertising `toxiproxy:1909N`, `CONNECT` on 9094 advertising `toxiproxy:1919N`. If both proxies pointed at 9092, the broker would advertise the app proxy to Connect and app-side faults would hit CDC too.
- Measurement never goes through Toxiproxy: the checker and runner read Postgres directly and reach Kafka on `INTERNAL` (9093, in-network) or `HOST` (published to the Mac as `localhost:2909N`, because the host cannot resolve `broker-N`).
- Kafka Connect uses its own internal topics (`connect-configs`, `connect-offsets`, `connect-status`), which are **not** reset per cycle; per-cycle connector names and slots keep cycles isolated.
- Broker data lives on a named volume per broker (`kafka-N`). Without it, recreating a broker wipes Connect's internal topics and Connect floods its log (finding F0).

# 17. Testing the harness itself

| Level | What | When |
|---|---|---|
| Unit | Checker golden fixtures; event schema; relay batching logic; crash guard | Every commit |
| Integration | One P1 cycle with `after_db_commit` must report exactly the expected lost count; one P3 cycle with no faults must report zero damage | Before every overnight run |
| Controls | C0-1, C0-3 per pattern | Before core experiments |

A run is only valid if all three levels pass on the same commit. Record the git commit hash in `run_id`.

# 18. Build order and acceptance criteria

| # | Milestone | Done when |
|---|---|---|
| 1 | Stack up | `make up` starts Postgres (primary + replica), Kafka, Connect, Toxiproxy on an M-series Mac |
| 2 | Order service P1 | Orders created via HTTP appear in Postgres and Kafka; markers logged |
| 3 | Checker v1 | Golden fixtures pass; no-fault P1 run reports zero damage |
| 4 | Crash points | A-1 by hand produces exactly the expected lost events |
| 5 | Runner v1 | A-1 × 30 runs unattended and writes 30 CSV rows |
| 6 | P2–P5 | All patterns pass C0-1 (one hour, zero damage) |
| 7 | Faults | Every fault type injects and recovers within its timeout |
| 8 | Consumers | Three strategies pass a no-fault run; G-1 shows the expected naive duplicates |
| 9 | Full catalogue | Must-run vectors complete across several nights and laptops (`SHARD=i/n`) with < 2% FAILED cycles, and `make merge` produces one `results.csv` |
| 10 | Analysis | `make report` regenerates all figures from `results.csv` |

# 19. Conventions

- Go: standard layout, `gofmt`, `go vet`, `golangci-lint`; errors wrapped with `%w`; contexts with timeouts on every network call.
- Every tunable is an environment variable with a documented default in `internal/config`; nothing is hard-coded.
- No component reads another component's internal state; all evidence comes from Postgres, Kafka and logs.
- Results, seeds, versions and the git commit are always recorded together. A number without them is not used in the paper.

# 20. Changes in versions 1.1 to 1.8

| # | Area | Change |
|---|---|---|
| 1 | 5.1, 10 | Payment tables are per strategy; the unique index exists only on the upsert table, so naive duplicates stay visible |
| 2 | 7, 12, 14.2 | Components are recreated, not restarted, whenever their environment changes, so a crash point is never re-armed |
| 3 | 5.3, 9.3 | P5 topic renamed with a `RegexRouter` transform |
| 4 | 9.2, 9.3 | `CONNECT_EOS` mapped to the worker (`disabled`/`enabled`) and connector (`required`) settings separately |
| 5 | 10 | Consumers share the P5 adapter; dedup keys on `event_key` (`order_id:seq` for P5) |
| 6 | 14.2 | RESET deletes the connector and slot before truncating |
| 7 | 12, 14.2 | Failover cycles rebuild primary and replica; scheduled last |
| 8 | 16 | One proxy per broker and per client class; Kafka Connect goes through its own proxy group |
| 9 | 16 | Memory figures for three brokers; Docker Desktop at 12 GB |
| 10 | 9.1, 11.2 | Window uses true commit times (`track_commit_timestamp`, `pg_xact_commit_timestamp`) |
| 11 | 15 | Load generator wording corrected |
| 12 | 14.1 | Full test catalogue delivered as `vectors.yaml` |
| 13 | 7, 11 | Hooked vs collateral damage reported separately for targeted crashes |
| 14 | 7.2 | Stall points for B-1, B-2, D-3; `on_log` fault triggers |
| 15 | 12 | Packet loss replaced by `reset_peer`; withheld acknowledgements via a downstream `timeout` toxic |
| 16 | 9.2, 10, 11 | All Kafka readers use `read_committed` |
| 17 | 6.4, 6.5 | Explicit delivery timeout (P2 retries depend on it); SIGTERM handling required for C0-3 |
| 18 | 11.1 | After failover the promoted replica is the source of truth |
| 19 | 14.2 | Run-time budget and controls that stop the run on failure |
| 20 | 16 | (1.2) Kafka Connect gets its own broker listener on 9094; `HOST` listener for the Mac; checker reads Postgres directly |
| 21 | 6.4 | (1.2) `KAFKA_REQUEST_OVERHEAD_MS`; documented franz-go behaviours (late hang detection, idempotent in-flight records, acks=1 needs idempotence off, partition-wide buffer failure) |
| 22 | 9.3 | (1.2) P4/P5 connector uses the `pg-connect` port 15433 |
| 23 | 14.2, 14.4, 18 | (1.2) Sharding across laptops, `make merge`, Milestone 9 reworded |
| 24 | 14.3 | (1.2) CSV columns `send_failed`, `host`, `shard` |
| 25 | 5.3 | (1.3) Cycle topics use `LogAppendTime`; the checker refuses others |
| 26 | 11.2, 14.3 | (1.3) `out_of_order` counts first deliveries only; new metric and CSV column `stale_redelivery` |
| 27 | 14.2 | (1.3) Sharding by vector, not by cycle |
| 28 | 14.2 | (1.3) Re-plan from measured overhead on 2–3 vectors before the full run |
| 29 | 7 | (1.4) Crash and stall points armed by SIGUSR1 after warm-up; randomised firing point `crash_after_n` |
| 30 | 7, 11.2, 14.3 | (1.4) `inflight_at_crash`, `oldest_pending_ms`, `load_s` columns |
| 31 | 14.2 | (1.5) Run-time budget updated with the overhead measured in Milestone 5 |
| 32 | 6.4 | (1.6) Buffer stays at 50,000 by decision; `KAFKA_MAX_BUFFERED_RECORDS` recorded per row in `env_json` |
| 33 | 14.2, 14.3 | (1.6) Pre-cycle health check with `pre_*` columns; preflight check |
| 34 | 16 | (1.6) Broker data on a named volume |
| 35 | 9.3 | (1.6) Connector settings the templates need beyond the listing |
| 36 | 11.2, 14.3 | (1.7) Measured relay batch sizes and `outbox_backlog_max` |
| 37 | 14.1 | (1.7) A-7 extended with load and poll-interval cells; effective-batch hypothesis |
| 38 | 9.3 | (1.7) `DEBEZIUM_POLL_INTERVAL_MS` set explicitly and recorded |
| 39 | 9.3 | (1.7) Connector names include the run ID |
| 40 | 14.1 | (1.8) A-3r and C-3r: connection-reset variants that trigger the retry path during the outage; originals kept |
| 41 | 14.1 | (1.8) A-7 prioritises poll-interval cells; 1,000/s cells depend on P-1 |
| 42 | 15 | (1.8) Analysis rule: report with and without flagged cycles, all cycles primary |
