---
title: "Technical Specification: Fault-Injection Study of Database–Kafka Consistency Patterns"
subtitle: "Go implementation · version 2.9 · November 2026"
---

# 1. Purpose and scope

This document specifies everything needed to build and run the study: the system under test, the fault-injection harness, the checker, the experiment runner and the data it produces. It is written for the team members who will implement it. **Go is the primary language** for every component we write; the only exception is the final statistics and plotting, where Python is recommended (Section 15).

## 1.1 Goals

1. Implement five database-to-Kafka synchronisation patterns (P1–P5) in one Go service, selectable by configuration.
2. Inject six classes of failure, at random moments and at exact code points, fully automated.
3. Measure lost, ghost, duplicate and out-of-order events, the inconsistency window, business damage and performance cost.
4. Run the test catalogue unattended and reproducibly on one machine. The catalogue has 52 vector IDs (48 that run cycles; the rest are aliases or unit tests) and 4,010 cycles: 2,125 `must`, 1,390 `should`, 420 `optional` and 75 `perf`. The paper run executes 3,590 of them (`must`, `should` and `perf`; Section 14.7). Development happens on Apple Silicon Macs; every paper result comes from one dedicated results machine (Sections 2.1 and 14.7).
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

## 1.4 Research questions and hypotheses

The study answers five research questions. Each hypothesis below is a prediction, fixed before the paper run, and is reported as supported, not supported or inconclusive with the test named here (Section 15.2). None is a finding until then. The working hypotheses already recorded in `FINDINGS.md` (H-REORDER, H-BATCH and the A-3, C-3 and B-3 expectations) are kept as written and sit under RQ2.

| RQ | Question | Main vectors |
|---|---|---|
| RQ1 | How do P1–P5 differ in event consistency under failures? | A, B, C, D, F |
| RQ2 | How do retry, outbox and CDC mechanisms affect event loss, duplication and ordering? | A-3/A-3r, A-4, A-7, B-1, B-2, B-5, C-1, C-3/C-3r, E-2, E-3 |
| RQ3 | How do the patterns differ in recovery time and inconsistency windows? | C-1, D-1, D-2, E-2, P-1 |
| RQ4 | How do consumer deduplication strategies affect business-level damage? | G-1 to G-4 |
| RQ5 | What are the performance costs of stronger consistency guarantees? | P-1 |

| # | Hypothesis | Measure (per cycle) | Test |
|---|---|---|---|
| H1 | When the order service crashes, at the exact point after the database commit (A-1) or at a random moment under load (A-6), P1 and P2 lose events in a non-zero share of cycles, and P3, P4 and P5 lose none | Share of cycles with `lost > 0` | Fisher's exact test, P1 and P2 each against P3–P5 pooled |
| H2a | During a broker outage (C-1, each outage length; the only outage vector that runs both P1 and P2), P2 loses fewer events than P1 but delivers more out of order (H-REORDER) | `lost`, `out_of_order + stale_redelivery` | Two-sided permutation test on cycle means; Cliff's delta |
| H2b | P3 duplicates after a relay crash between publish and mark (A-4) grow with `RELAY_BATCH_SIZE`; P4 and P5 duplicates after a Connect restart (E-2) grow with `OFFSET_FLUSH_MS` | `duplicates` | Kendall's tau between sweep level and per-cycle value, one-sided (positive), p-value by permutation |
| H2c | With two or more relays (B-1, B-2, B-5), P3 delivers some events out of order in a non-zero share of cycles; with one relay (every other P3 vector, A-4 included) no first delivery is out of order | `out_of_order` | Fisher's exact test on the share of cycles with `out_of_order > 0`, two or more relays against one |
| H3a | With no fault, the inconsistency window is shortest for P1 and P2, longer for P3 and longest for P4 and P5, at every load level of P-1 | `window_p50_ms` | Kruskal–Wallis per load level, then pairwise permutation tests |
| H3b | After a fault, P3, P4 and P5 always reach consistency within the observation period; P1 and P2 have unrecovered cycles | `consistency_recovered`, `consistency_recovery_s` | Kaplan–Meier curves per pattern; log-rank test |
| H4 | When duplicates reach the consumers (G-1, G-3), only the naive strategy makes duplicate payments; when offsets are committed before the effect (G-2), every strategy can miss payments | `duplicate_payments_<s>`, `missing_payments_<s>` | Fisher's exact test per strategy pair |
| H5 | At every load level, P3, P4 and P5 cost more database CPU per 1,000 committed changes than P1 and P2, and P4 and P5 retain more WAL | `db_cpu_s_per_1000_changes`, `wal_retained_bytes_max` | Permutation test on cycle means per load level; ratio with bootstrap CI |

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

All images must have both `arm64` builds (development on Apple Silicon Macs) and `amd64` builds (the results machine). Image IDs differ between the two architectures, so `make merge` refuses to combine rows from the two kinds of machine, and no paper result mixes them.

## 2.1 Results machine

| Item | Value |
|---|---|
| Hardware | One x86-64 desktop PC, 16 GB RAM, on mains power, used by nobody else during a session |
| OS | Windows 10 or 11 with WSL2, distribution Ubuntu 24.04; everything below runs inside WSL2 |
| WSL2 VM | `.wslconfig`: `memory=13GB`, `swap=8GB`, `vmIdleTimeout=-1`; `systemd=true` in `/etc/wsl.conf` |
| Docker | Docker Engine inside WSL2, not Docker Desktop |
| Go | 1.27.3, `linux/amd64` |
| Repos | `~/capstone` and `~/dualwrite-results`, side by side on WSL2's own ext4 filesystem, never under `/mnt/c` |
| Access | SSH over Tailscale from the development Mac; a run lives in a `tmux` session named `study` |
| Windows | Sleep and hibernate off on AC; updates paused for the session; a start-up task runs `wsl.exe -d Ubuntu-24.04 --exec sleep infinity` so WSL2 returns after a reboot; the WSL2 disk (`ext4.vhdx`) excluded from Defender real-time scanning |

`deploy/host-info.sh` writes the machine's CPU model, core count, RAM, Windows build, WSL2 kernel and Docker Engine version to `host-info.txt` in the run directory before the first cycle. The paper's environment section quotes it.

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
├── results/                # results.csv, damage/, logs/ (git-ignored; backed up to dualwrite-results)
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
CREATE INDEX outbox_unsent ON outbox (created_at, id) WHERE sent_at IS NULL;   -- matches the relay's ORDER BY (Section 8)

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

`event_key` is the `event_id` for P1–P4 and `"<order_id>:<seq>"` for P5, which has no event ID (Section 10).

**Commit times.** The commit time of each version is read with `pg_xact_commit_timestamp(xmin)` on its `orders_history` row (needs `track_commit_timestamp = on`, Section 9.1). The history row is inserted by the trigger in the same transaction as the change and never updated, so its `xmin` is the change's transaction. Safeguards, because a wrong timestamp would silently distort the window:

- **Read while available.** The checker reads every commit time in CHECK of the same cycle, minutes after the commit. Commit timestamps are kept only for transactions newer than the cluster's oldest frozen XID; the runner records `xid_age = age(datfrozenxid)` of the database at RESET, and START refuses a cycle when it exceeds 150 million (the default `autovacuum_freeze_max_age` is 200 million). A run uses far fewer: about 10,000 transactions per cycle, about 40 million for a whole study.
- **Freezing cannot reach the rows read.** RESET truncates `orders_history` every cycle, so no row read by the checker is older than its cycle, and autovacuum's freeze threshold (`vacuum_freeze_min_age`, 50 million transactions) is never reached within one. As a second line, an integration test runs `VACUUM (FREEZE)` on a filled `orders_history` and checks that every `pg_xact_commit_timestamp(xmin)` is unchanged (since PostgreSQL 9.4 freezing keeps the raw `xmin`; the test confirms it on the pinned version).
- **Both servers record it.** START checks `SHOW track_commit_timestamp` returns `on` on the primary and on the replica, and fails the cycle (`FAILED_START`) otherwise. A standby records the commit times carried in the WAL commit records it replays, so after a failover the promoted replica (the truth, Section 11.1) reports the old primary's original commit times for replayed transactions, and its own for transactions committed after promotion. Both servers run on the same host and share its clock.
- **Missing or impossible values are never guessed.** A version with a NULL commit time is counted in `commit_ts_missing` and excluded from the window. A version whose first broker timestamp is earlier than its commit time by more than 5 ms (the clock tolerance) is counted in `window_negative` and excluded too. If either count is above zero, the window columns and `consistency_recovery_s` are left empty for that cycle and `window_valid` is `false`; the other metrics remain valid, because loss, duplicates and ordering do not use commit times. `pre_flag` is not used for this: it stays a machine-health flag (Section 14.2).

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
- A fresh topic per cycle keeps cycles isolated. Each cycle's topic is dumped, verified and deleted in that cycle's DUMP state (Section 14.2), so the broker's partition count stays flat; before 2.8 topics were kept until the end of a run, which caused harness bug 12.
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
                      WHERE sent_at IS NULL ORDER BY created_at, id
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
| `RELAY_DB_TIMEOUT_MS` | 5000 | Limit on each relay database call. Matches the order service's request limit, so both react alike to a dead database, whose connections hang rather than fail |
| Number of relay replicas | 1 (2/4 for ordering tests) | B-1, B-5, H-3 |

The relay holds its transaction open while publishing so the row locks persist. This is intentional: it is what makes B-2 (lock released by Postgres's `idle_in_transaction_session_timeout` during a pause) possible.

**Read order.** `created_at` is `now()`, the start time of the writing transaction, so two rows can share it, and the order among equal timestamps would otherwise be whatever plan Postgres picks. `id` breaks ties, so a batch's content and order are deterministic for a given table state. `id` is a random UUID, so the tie-break is arbitrary between orders. It never separates two versions of one order: the load generator sends an order's next change only after the previous request has returned, so the later version's transaction starts after the earlier one committed, and its `created_at` is strictly later.

**Per-order ordering: what P3 does and does not guarantee.**

- *One relay:* each order's events are published in version order. Batches are read oldest first, published in read order, acknowledged before the next batch is read, and all of one order's events go to one partition. A relay crash between publish and mark (A-4) republishes a batch, which yields duplicates and stale redeliveries (`stale_redelivery`) but no first delivery out of order.
- *Two or more relays:* **no per-order ordering is guaranteed.** `FOR UPDATE SKIP LOCKED` lets a second relay skip rows that a stalled or slow relay has locked and publish newer rows first: version 2 of an order can reach Kafka before version 1. The paper must not claim per-order ordering for P3 with more than one relay. B-1 (a stall at `after_read_before_publish` on relay-1 of two) and B-2 (two relays, with a lock released by `idle_in_transaction_session_timeout`) are the tests that show it, and B-5 measures how the effect grows with 2 and 4 relays (hypothesis H2c). In the excluded study-v3 pilot, 13 of 13 B-1 cycles and 9 of 10 B-2 cycles had out-of-order events, while about 250 one-relay P3 cycles across 16 vectors had none; that motivates the tests and is not a result.

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

`heartbeat.interval.ms` is 10,000 ms (`DEBEZIUM_HEARTBEAT_MS`), recorded in `env_json`. With Debezium's default of 0 and Postgres's 60 s `wal_sender_timeout`, the replication connection was dropped 4 times in a no-fault hour on P4 (finding F31). Heartbeats go to Debezium's own topic, created through Connect's `topic.creation.default.*` settings; the checker and consumers read only `orders.<cycle_id>`.

`retriable.restart.connector.wait.ms` stays at Debezium's default of 10,000 ms, set from `DEBEZIUM_RETRY_WAIT_MS` and recorded in `env_json`. When a task enters this wait, deleting the connector takes about 10 s, which extends the failover loss window. Every report of CDC recovery times must state this setting.

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

## 9.4 CDC progress evidence (used by DRAIN, Section 14.2)

Stable Kafka end offsets do not prove that Debezium has caught up. A connector waiting in `retriable.restart.connector.wait.ms` (10 s) or a task restarting after E-2's Connect restart writes nothing for longer than the drain's 10 s quiet period, so offsets look stable while committed changes are still unstreamed. The checker would then count them as lost. For P4 and P5 the drain therefore needs positive evidence:

| Evidence | Read from | Required for drained |
|---|---|---|
| Connector and task state | Connect REST `GET /connectors/<name>/status` | Connector `RUNNING` and every task `RUNNING` |
| Replication slot | `SELECT active, confirmed_flush_lsn, restart_lsn FROM pg_replication_slots WHERE slot_name = $1` | `active = true` |
| Drain markers | Marker rows committed after load stops, until **every partition** of the cycle topic has received one (below), and the WAL position `drain_target_lsn = pg_current_wal_lsn()` read right after the last marker's commit | A marker is in each partition of `orders.<cycle_id>` |
| Kafka end offsets | `ListEndOffsets` on the cycle topic | Unchanged for `quiet_s` after the marker arrived |

**Why markers, one per partition, and not the slot's LSN.** Debezium reads transactions in commit order through one task and hands them to Connect's producer in that order. Kafka keeps order only **within** a partition, though: a record for partition 2 can still be retrying after a later marker has landed in partition 0. So one marker proves nothing about the other partitions. A marker in a partition proves that every earlier record for **that** partition has been written, because Connect's producer is idempotent (Kafka's default since 3.0, `acks=all` in every paper-run cell) and keeps order within a partition. Once each partition has a marker, every load commit has been streamed and written. The slot's `confirmed_flush_lsn` cannot serve as the gate: Debezium advances it only when Connect commits offsets, every `OFFSET_FLUSH_MS` (up to 60 s, swept in E-2), so it lags by design. It is recorded at the end of the drain (`drain_slot_lsn`) next to `drain_target_lsn` as supporting evidence, not as a condition.

**The markers.** P4: a row in `outbox` with `aggregatetype = 'order'`, `type = 'DrainMarker'`, `aggregateid` = a marker ID. P5: a row in `orders` with `order_id` = a marker ID, `status = 'MARKER'`. Marker IDs are UUIDs drawn in sequence from the cycle seed. The runner commits one marker at a time, sees which partition it reached, and commits the next until every partition has one; the partition depends on how the connector serialises the key, so the runner observes it rather than predicting it. With 3 partitions this takes about 5 markers; more than 20 is `FAILED_DRAIN` (`drain_reason = marker_partitions`). The IDs are recorded in `drain_marker_ids`. The checker, the consumers, the window, the business-damage comparison and the drain's payment count all exclude them by ID, and their `orders_history` rows are excluded from DB. A golden test covers the exclusion.

**Failure.** If the evidence is not complete within `drain.timeout_s` (120 s) after load stops, the cycle is `FAILED_DRAIN` with `drain_reason` = `connector_not_running`, `slot_inactive`, `marker_missing` or `offsets_moving`, the first condition that was still unmet. Its logs, connector status and slot row are kept. A broker that drops out during the drain is waited for as in Section 14.2, which does not count towards the quiet period.

P3 uses a single marker in `outbox`, in addition to the existing condition that no row is unsent. One is enough there: the relay reads in `created_at, id` order and waits for every record of a batch to be acknowledged before reading the next, so when the marker arrives, every earlier row on every partition has been acknowledged. P1 and P2 publish from the order service itself, which keeps running after load stops. Every record it still buffers is delivered or failed within `KAFKA_DELIVERY_TIMEOUT_MS` (5 s), shorter than the 10 s quiet period, and P2's retries add at most `RETRY_MAX` back-offs, logged as `RETRY_SCHEDULED`. Their drain therefore keeps the offset and payment conditions, and also waits until no retry is scheduled for later than the current time.

The P4 marker's `payload` is a valid event (Section 5.2) of type `DrainMarker`, so the shared adapter decodes it; consumers treat it as a non-payment event.

# 10. Consumers (`cmd/consumer`, group G)

One binary, `CONSUMER_STRATEGY = naive | dedup | upsert`. In group G cycles all three run **in parallel**, each in its own container, its own consumer group (`g-<strategy>-<cycle_id>`) and its own tables, so they see the identical event stream. Manual offset commits (auto-commit off), `isolation.level=read_committed`. `consumers_per_group` (default 1, 2 for G-4) sets the replicas per strategy.

| Strategy | Database side effect of one payment-triggering event |
|---|---|
| naive | `INSERT INTO consumer.payments_naive (order_id, event_key, amount)`, one statement in its own transaction |
| dedup | **One transaction**: `INSERT INTO consumer.processed_events (event_key) ... ON CONFLICT DO NOTHING`; if no row was inserted, end without a payment (the event was seen before); otherwise `INSERT INTO consumer.payments_dedup ...`; then `COMMIT`. The processed-event row and the payment row commit together or not at all, so a crash can never record the event as processed without its payment, or the payment without the record |
| upsert | `INSERT INTO consumer.payments_upsert ... ON CONFLICT (order_id) DO NOTHING`, one statement in its own transaction |

**Processing order.** `CONSUMER_COMMIT_ORDER` fixes the sequence for every strategy. The Kafka offset commit is synchronous (`CommitRecords`, which returns after the broker has accepted it).

| Step | `effect_first` (default; G-1) | `offset_first` (G-2) |
|---|---|---|
| 1 | Apply the database side effect | Commit the Kafka offset |
| 2 | Commit the database transaction | Crash hook `after_commit_before_side_effect` |
| 3 | Crash hook `after_side_effect_before_commit` | Apply the database side effect |
| 4 | Commit the Kafka offset | Commit the database transaction |

The hook names refer to the **Kafka offset commit**. `after_side_effect_before_commit` fires after the database transaction has committed and before the offset commit, so a crash there redelivers an event whose effect is already stored: duplicates for naive, none for dedup and upsert. `after_commit_before_side_effect` fires after the offset commit and before any database work, so a crash there loses the effect for every strategy: missing payments. Events that trigger no payment, and undecodable records (logged), have their offset committed without a side effect.

**Input adapter.** Consumers share the checker's event adapter (`internal/events`): P1–P4 events are read as-is; P5 Debezium envelopes are mapped from `after.*`. A payment is triggered by an `OrderPaid` event (P1–P4) or a P5 row change whose `after.status` is `PAID`. `event_key` is `event_id`, or `"<order_id>:<seq>"` for P5.

`CONSUMER_COMMIT_ORDER = effect_first | offset_first` selects G-1 vs G-2 behaviour. Business damage is computed per strategy (Section 11).

# 11. Checker (`cmd/checker`, `internal/checker`)

## 11.1 Inputs

- Postgres: all rows of `orders` (final state and version) and the full `outbox` (for P3/P4 provenance).
- Kafka: every record of `orders.<cycle_id>` from offset 0 on all partitions, with partition, offset and broker timestamp, read with `isolation.level=read_committed`.
- Logs: the cycle's `SENT_CALLED` / `SEND_ACKED` markers (for loss attribution).
- Consumer schema: every row of the three payment tables (`payment_id`, `order_id`, `event_key`, `amount`), and `orders.amount` and final status from the truth (group G only).
- The `CRASH_POINT_HIT` marker (hooked events).
- The runner's timestamps: `fault_end_at` (the moment the last fault's `Recover()` returned and the recovered components were healthy), load stop, and `observed_until` (end of DRAIN, drained or timed out).
- The load generator's per-second series: requests issued and requests succeeded, by second of the cycle.
- The cycle's `drain_marker_ids`, excluded from every input above.

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
| window (p50/p99/max) | broker timestamp of first occurrence − commit time of that version, `pg_xact_commit_timestamp(xmin)` of its `orders_history` row (not `now()`, which is the transaction start). Versions without a valid commit time are excluded and counted (Section 5.1); if any are, the window is left empty and `window_valid = false` |
| window_p99_pre_ms | window p99 over versions committed between the end of warm-up and the first fault: the cycle's own baseline for `synchronization_recovery_s` |
| service_recovery_s | from `fault_end_at` to the start of the first 5 s stretch in which every second has at least 95% of issued requests succeeding. 0 if the service never degraded |
| synchronization_recovery_s | from `fault_end_at` to the earliest time *t* such that every version committed in each following 10 s interval up to load stop has a first delivery within `window_p99_pre_ms` + 1,000 ms: the moment newly committed changes are published promptly again |
| consistency_recovery_s | from `fault_end_at` until every version committed before `fault_end_at` has a first delivery in Kafka: max(first delivery − `fault_end_at`) over those versions, or 0 if all were delivered before the fault ended |
| `<x>_recovered` | for each of the three recovery times: `true` if the condition was reached by `observed_until`; `false` if it was not; empty for cycles without a fault. **When `false`, `<x>_recovery_s` holds the censoring time (`observed_until` − `fault_end_at`, or load stop − `fault_end_at` for service and synchronization), never an invented recovery time**, and analysis treats it as right-censored (Section 15.2). A P1 cycle that permanently loses an event after a commit and crash is therefore `consistency_recovered = false`, not a long recovery |
| lost_after_send | lost pairs whose event has a `SENT_CALLED` marker (producer-buffer loss) |
| lost_after_ack | lost pairs whose event has a `SEND_ACKED` marker: acknowledged by the broker, then lost. Measured for P1–P3; empty for P4 and P5, which publish through Connect and log no delivery markers |
| business damage, per strategy | per-order comparison of payment rows against expected effects, five disjoint classes (below); `business_damage_<s>` is their sum |
| hooked_lost / hooked_dup | damage to the event(s) named in `CRASH_POINT_HIT` |
| collateral_lost / collateral_dup | the same metrics for every other event |
| inflight_at_crash / oldest_pending_ms | records with `SENT_CALLED` but no outcome at `CRASH_POINT_HIT`, and the age of the oldest |
| relay_batches / batch_rows_mean / batch_rows_max | number of relay batches and the rows they actually held, from `RELAY_BATCH` markers (`phase=read`). The effective batch can be far below `RELAY_BATCH_SIZE` |
| outbox_backlog_max | maximum unsent outbox rows, sampled every second (P3), so a relay falling behind is measured rather than inferred from the window |
| failover_at / failover_lag_bytes / failover_lag_ms | when the primary was killed, and replication lag read just before it (D vectors) |
| wal_retained_bytes_max | maximum `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)` of the cycle's slot, sampled every second (P4/P5) |

**How long recovery can be observed.** Service and synchronization recovery need traffic, so they can only be observed until load stops, `post_fault_s` (default 20 s) after the fault ends: synchronization recovery is censored at about 10 s, because it needs one full 10 s interval of prompt deliveries. Consistency recovery can be observed until the end of DRAIN (up to `timeout_s`, 120 s, after load stops). These limits are part of every recovery result; a censored cycle means "not recovered within this window", not "never recovers". A vector that needs longer observation sets a larger `post_fault_s` in `vectors.yaml`; v2.9 changes no vector.

**Version history:** DB is built from `orders_history` (Section 5.1), which a trigger fills on every insert/update of `orders`. The trigger runs in all patterns, so its overhead is equal across them.

**Business damage, per order.** Versions 2.8 and earlier computed `|payment rows − PAID orders|`, under which one missing payment and one duplicate payment cancel to zero. From 2.9 every payment row and every expected payment is classified, separately for each strategy *s* ∈ {naive, dedup, upsert}, and the classes are counted, never netted against each other.

*Expected effects.* E = the orders whose final status in the truth is `PAID` (marker excluded). Each order *o* in E expects exactly one payment with `order_id = o`, `amount = orders.amount` of *o*, and an `event_key` that resolves to *o*'s PAID version. A key resolves through the cycle's Kafka records: for P1–P4 the record whose `event_id` equals the key gives its `(order_id, seq)`; for P5 the key is `"<order_id>:<seq>"` itself.

*Classification.* Each row of `consumer.payments_<s>`, in `payment_id` order, falls into the first class that applies:

| Order | Class (CSV column `<class>_<s>`) | Row condition |
|---|---|---|
| 1 | `unexpected_payments` | `order_id` is not in E: the order was never paid in the truth, or does not exist there (for example a ghost after failover) |
| 2 | `incorrect_order_mappings` | the `event_key` does not resolve, resolves to a different order than the row's `order_id`, or resolves to a version of the order that is not its PAID version |
| 3 | `incorrect_payment_amounts` | `amount` differs from the order's `amount` in the truth |
| 4 | `duplicate_payments` | a correct row (none of the above) for an order that already has an earlier correct row |
| — | correct | the first correct row for its order |

Then `missing_payments_<s>` = the orders in E with no correct row. An order whose only row has a wrong amount counts once in `incorrect_payment_amounts` and once in `missing_payments`, because both happened: the wrong effect exists and the right one does not.

`business_damage_<s>` = `missing_payments_<s>` + `duplicate_payments_<s>` + `unexpected_payments_<s>` + `incorrect_payment_amounts_<s>` + `incorrect_order_mappings_<s>`. Each row is in at most one row class and each order in E at most once in `missing_payments`, so a duplicate can never hide a missing payment. `damage/<cycle_id>.json` lists the order IDs and payment IDs behind every class.

```go
func businessDamage(truth map[uuid.UUID]Order, rows []PaymentRow, resolve func(key string) (Pair, bool)) Damage {
    var d Damage
    paid := map[uuid.UUID]bool{}            // orders with a correct row so far
    for _, r := range rows {                 // ordered by payment_id
        o, inE := truth[r.OrderID]
        switch {
        case !inE || o.Status != "PAID":
            d.Unexpected = append(d.Unexpected, r.PaymentID)
        case !resolvesTo(resolve, r.EventKey, r.OrderID, o.PaidSeq):
            d.WrongMapping = append(d.WrongMapping, r.PaymentID)
        case !r.Amount.Equal(o.Amount):
            d.WrongAmount = append(d.WrongAmount, r.PaymentID)
        case paid[r.OrderID]:
            d.Duplicate = append(d.Duplicate, r.PaymentID)
        default:
            paid[r.OrderID] = true
        }
    }
    for id, o := range truth {
        if o.Status == "PAID" && !paid[id] {
            d.Missing = append(d.Missing, id)
        }
    }
    return d
}
```

**Database cost (RQ5).** Measured over one interval, from the end of warm-up to load stop (`cpu_window_s`), so CPU and work cover the same time:

| Metric | Definition |
|---|---|
| `db_cpu_s` | CPU seconds used by the `postgres-primary` container in the interval: the difference of Docker's cumulative `cpu_stats.cpu_usage.total_usage` (nanoseconds) at its two ends, ÷ 10⁹. It includes every Postgres process in the container (backends, the history trigger, WAL senders for CDC and the replica, autovacuum, checkpointer); the replica's own container is not counted |
| `committed_changes` | `orders_history` rows (marker excluded) whose commit time lies in the interval, read on the primary: one per committed order change (create, pay or cancel) |
| `db_cpu_s_per_1000_changes` | `db_cpu_s / committed_changes × 1000` |
| `changes_per_s` | `committed_changes / cpu_window_s` |
| `orders_per_s` | version-1 rows among them ÷ `cpu_window_s`: new orders, about 56% of changes at the default mix (1 create + 0.8 follow-ups per order) |
| `db_cpu_pct` | `db_cpu_s / cpu_window_s × 100`, where 100 is one full core; same interval as above |
| `throughput_ops` | successful HTTP requests per second from the load generator. A request is one change, but a request that failed at the client may still have committed, so `changes_per_s`, not `throughput_ops`, is the denominator for cost |

Cycles with a failover have no single primary over the interval, so these columns are empty there. RQ5 uses only P-1, which has no faults.

## 11.3 Outputs

- One result struct appended to `results/results.csv` by the runner.
- `results/damage/<cycle_id>.json`: the exact `(order_id, seq)` pairs for every damage class, used for root-cause analysis. Lost pairs also carry their commit time, the order's final version and whether they are intermediate versions.
- `raw/<cycle_id>/` for **every** cycle, whatever its status: everything the checker read, so every metric can be recomputed without running the cycle again (Section 11.5).

## 11.5 Raw data and recomputation

Every cycle saves its inputs, not only its results. Before RESET of the next cycle deletes them, the runner writes:

| File | Content | Needed for |
|---|---|---|
| `dumps/<cycle_id>.jsonl.gz` | Every offset of the cycle topic (Section 14.2, DUMP) | Loss, duplicates, ordering, window, recovery |
| `raw/<cycle_id>/truth.jsonl.gz` | `orders` (id, status, version, amount), `orders_history` (order_id, version, status, `xmin`, commit time) and `truth_source`, read from the truth database in CHECK | Everything compared with the truth; commit times survive the next RESET's truncate |
| `raw/<cycle_id>/outbox.jsonl.gz` | Every `outbox` row: id, aggregateid, seq, `created_at`, `sent_at` (P3, P4) | Relay and outbox metrics |
| `raw/<cycle_id>/consumer.jsonl.gz` | Every row of the three payment tables and `processed_events` (group G) | Business damage, all classes |
| `raw/<cycle_id>/load.csv.gz` | Per second: requests issued and succeeded; per request: start time, latency, outcome | Service recovery, latency, throughput |
| `raw/<cycle_id>/markers.jsonl.gz` | Every marker line of Section 13 from every component, kept for every cycle (full logs are still kept only for damaged or failed cycles) | `lost_after_send`, `lost_after_ack`, hooked and collateral damage, in-flight at crash, relay batches |
| `raw/<cycle_id>/timeline.json` | The cycle's seed and settings; every state's start and end; each fault's inject and recover times; `fault_end_at`, load stop, `observed_until`; the two CPU samples (cumulative nanoseconds and their times); WAL-retained, outbox-backlog and slot samples; drain evidence (connector status, slot rows, marker partitions and offsets, LSNs); broker restarts | Cost, recovery, drain, failover lag, every runner-measured column |

About 1 MB per cycle, about 3.6 GB for the paper run.

**Recomputation.** `cmd/recompute` reads a session directory's raw files and writes every metric column again, in `results.recomputed.csv`. Rules:

1. **At the run commit it must reproduce `results.csv` exactly** for every metric column. This is a go criterion of the check run (Section 14.7) and is checked again on every finished session: it proves that the raw data is complete and that the checker depends on nothing else.
2. A bug in a **metric definition or the checker** may be fixed afterwards: the fix is committed, recorded in `FINDINGS.md` as a harness bug, and `cmd/recompute` is run at that commit. Every recomputed row carries `analysis_commit`, and the paper names both the run commit and the analysis commit.
3. A bug in **what was observed** (fault injection, timing, load, drain, the dump or the raw capture itself) cannot be fixed this way. It needs a new run (Section 14.7).
4. `results.csv` is never edited. The recomputed file sits beside it.

## 11.4 Correctness requirements (control C0-2)

Golden tests in `internal/checker/testdata/`: hand-written DB and Kafka fixtures with known damage. `go test ./internal/checker/...` must pass before any experiment run, and runs in CI on every change. Required fixtures: no damage; 2 lost + 2 duplicates + 1 out-of-order + 1 ghost; a triple duplicate (counts as 2); 100,000 events with 50 planted problems. From 2.9 also:

| Fixture | Expected result |
|---|---|
| One missing and one duplicate payment in the same strategy | `missing_payments = 1`, `duplicate_payments = 1`, `business_damage = 2` (the 2.8 formula gave 0) |
| One payment each: for an unpaid order; with a key resolving to another order; with a wrong amount | 1 in each matching class; the wrong-amount order also counts as missing |
| Drain markers in DB, Kafka and the payment input | Excluded everywhere; every metric as without it |
| A version with a NULL commit time; a version delivered 10 ms before its commit time | `commit_ts_missing = 1`, `window_negative = 1`, `window_valid = false`, window columns empty |
| A P1 cycle with one version never delivered | `consistency_recovered = false`, `consistency_recovery_s` = censoring time |
| A cycle whose service drops below 95% success for 7 s after the fault | `service_recovery_s` = start of the first full 5 s healthy stretch |

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
| `failover` | Kill primary → `SELECT pg_promote()` on the replica → the replica joins the `failover` network with the alias `postgres-primary`, so both pg proxies reach it by the name they already use → restart `toxiproxy-pg` → apply `CDC_FAILOVER_RECOVERY` (Section 9.3). Applications reconnect through the same address. Updating a live proxy's upstream stalls under load, because Toxiproxy dials upstream in its accept loop. The primary must die first: cutting traffic beforehand would let the replica catch up and hide ghost events |
| `kill_leader` | Look up the leader of partition 0 with `kadm`, SIGKILL that broker |

**Network fault rule.** A network fault targets the publisher's path: `kafka-app` for P1–P3 and `kafka-connect` for P4–P5. Postgres latency uses `pg-app` for P1–P3 and both pg groups for P4–P5 (F-4, F-5).

**After every failover cycle** (`requires_rebuild: true`) the runner rebuilds the original setup: recreate the primary from scratch (init scripts), and re-seed the replica with `pg_basebackup`. Recreating the replica removes the alias, so the proxies reach the new primary again. The runner schedules all `requires_rebuild` vectors at the end of a run.

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
| RESET | First wait until the broker serves (metadata lists every broker and a controller), because one restarted by the previous cycle may still be reloading its logs; record `broker_partitions`, `delete_dirs` and `delete_dirs_kb`, and `xid_age` (Section 5.1). Then, in this order: (1) stop load and app components; (2) delete the previous connector and drop its replication slot; (3) truncate all tables (incl. `orders_history`, consumer schema); (4) create topic `orders.<cycle_id>`; (5) reset Toxiproxy (remove toxics, enable proxies). Deleting the connector before truncating keeps a live connector from streaming the truncate | 60 s |
| START | **Recreate** components with the cycle's env (PATTERN, CRASH_AT/STALL_AT, sweep values, topology); recreate Kafka Connect if its worker config changed; create the connector (P4/P5); wait for `/healthz`. Check that `track_commit_timestamp` is `on` on the primary and the replica and that `xid_age` is below 150 million (Section 5.1); otherwise `FAILED_START` | 90 s |
| WARMUP | Start loadgen; wait `warmup_s` | — |
| FAULT | Targeted: wait for `CRASH_POINT_HIT` / `STALL_POINT_HIT`. Random: inject each fault at uniform(`at_s`) or on its `on_log` marker | 60 s after the last scheduled fault |
| RECOVER | `Recover()` each fault; recreate crashed components with crash/stall settings cleared (recovery rule, 14.1) | 90 s |
| DRAIN | Stop load; read CPU and the interval end for the cost metrics (Section 11.2). Commit the drain markers (P3–P5; Section 9.4) and record `drain_target_lsn`. Wait until the cycle topic's partitions all have a leader and answer ListOffsets. Then the cycle is drained when **all** of these hold: (a) Kafka end offsets, the unsent outbox count (P3) and the payment count (group G) are unchanged for `quiet_s`; (b) P3: the marker is in the topic; P4–P5: a marker is in every partition; (c) P4–P5: connector and every task `RUNNING` and the slot `active`; (d) P2: no retry scheduled for later than now. Stable offsets alone never end the drain for P3–P5. If the broker drops out, wait for it again and restart the quiet period: an unreachable broker never counts as quiet. Record `drain_s`, `drain_slot_lsn` and, on failure, `drain_reason` | `timeout_s` (120 s) from load stop; each broker wait up to 180 s. Expiry is `FAILED_DRAIN` (Section 9.4) |
| CHECK | Run checker library in-process | 120 s |
| DUMP | Read the brokers' logs since cycle start and record each restart as `clean` or `unclean` with its reload time. Delete the CDC heartbeat topic. Wait for the broker, then dump every offset of `orders.<cycle_id>` to `dumps/<cycle_id>.jsonl.gz` and verify it (definitions below). Only then delete the topic, and wait until it has left the metadata. A failed dump keeps the topic and marks an OK cycle `FAILED_DUMP`. This keeps the broker's partition count flat through a run (harness bug 12, finding F32) | 180 s broker wait, 120 s dump, 30 s delete |
| RECORD | Append CSV row; write damage JSON; keep logs if damage > 0 or status ≠ OK | — |
| REBUILD | Only for `requires_rebuild` vectors (Section 12) | 180 s |

**Dump definitions.** The dump is the raw log, not the committed view the checker reads, so it uses a separate client with `FetchIsolationLevel(ReadUncommitted())` and `KeepControlRecords()`. With those two options franz-go returns every record batch the broker holds, including aborted transactional records and the control records (commit and abort markers) that it otherwise discards; no raw log-segment reading is needed. Each control record occupies one offset, and the cycle topics use `cleanup.policy=delete` with retention far longer than a cycle, so between a partition's log start offset and its end offset every offset holds exactly one record. The definitions are:

- `dump_records` = data records + control records written, over all partitions;
- `dump_control_records` = the control records among them (non-zero only when Connect writes transactionally, `CONNECT_EOS=on`);
- verification: for every partition, records written = end offset − log start offset, and the offsets are consecutive. Any gap, or any count that differs, fails the dump.

The checker's `events_total` counts committed data records only, so it equals `dump_records − dump_control_records −` aborted data records. The analysis does not compare them directly.

**Pre-cycle health check.** During the last 5 s of warm-up, before any fault is armed, the runner samples the host's one-minute load average (`/proc/loadavg` on Linux, `sysctl vm.loadavg` on macOS), Postgres CPU and p99 latency, and records them as `pre_host_load`, `pre_db_cpu_pct` and `pre_p99_ms`. A cycle above `PRECHECK_MAX_P99_MS` (default 30) or `PRECHECK_MAX_HOST_LOAD` (default 8) gets `pre_flag` set to `latency`, `load` or both. It flags and never stops, so a spike can be traced to the machine or to the system.

**No traffic means invalid.** If no request succeeded in the 5 s before a fault, the cycle is marked `INVALID_NO_LOAD`, because a fault on an idle system proves nothing. Every row records failed requests in `load_failed`.

**Preflight.** The run refuses to start if Connect's internal topics are missing or the disk has under 20 GB free.

Any timeout marks the cycle `FAILED_<STATE>`, saves its logs and moves on; one bad cycle never stops the run. A violated control `assert` marks `CONTROL_FAIL` and **does** stop the run. The runner is resumable: it skips cycles already present in `results.csv`.

**Sharding.** `--shard i/n` runs only the vectors whose `hash(vector_id) mod n = i-1`, so n laptops split the catalogue with no overlap and every cycle of a vector runs on one machine, and each writes `results/results.<host>.csv`. All laptops must use the same git commit and image digests (checked at start and recorded in every row). Performance vectors (`group: perf`) are never sharded: they run on one designated machine. Sharding is kept for development; the paper run does not use it (Section 14.7).

**Run-time budget.** Measured on the excluded study-v3 run (755 cycles on the development Mac): 20.2 h in all, of which 17.0 h was load and the rest about 15 s of fixed overhead per cycle (reset, start, drain, check). The runner's `-dry-run` defaults to 75 s and overstates the total; pass `-overhead 15`. On that figure, with no cuts to repeats or durations:

| Session | Cycles | Load | Overhead | Rebuilds (unmeasured) | Estimate |
|---|---|---|---|---|---|
| `must` | 2,125 | 39.3 h | 8.9 h | D-1, D-2: 380 cycles | 50–68 h |
| `should` | 1,390 | 23.3 h | 5.8 h | D-3: 20 cycles | 29–30 h |
| `perf` | 75 | 2.7 h | 0.3 h | — | 3 h |
| **Paper run** (`must` + `should` + `perf`) | **3,590** | **65.3 h** | **15.0 h** | 400 cycles | **82–101 h** |
| `optional` (not run in the paper run) | 420 | 6.0 h | 1.8 h | D-4, H-2 | 8–10 h |
| Whole catalogue | 4,010 | 71.3 h | 16.8 h | | 90–111 h |

The range comes from the REBUILD state, never yet timed on a full run (up to its 180 s cap). The v2.9 check run times it, along with the DUMP state and the CDC drain evidence (Sections 9.4 and 14.7), which add time per cycle that v3 did not have: at least the marker's trip through the pipeline, and for P4 and P5 up to Debezium's 10 s retry wait after a Connect fault. The plan is re-checked against the deadline before the freeze.

## 14.3 Results CSV (one row per cycle)

```
run_id, cycle_id, vector_id, pattern, mode, fault, crash_point, repeat, seed,
status, sweep_json, env_json, versions_json, load_rate, orders_total, events_total,
lost, ghost, duplicates, out_of_order, stale_redelivery, lost_after_send, lost_after_ack,
hooked_lost, hooked_dup, collateral_lost, collateral_dup,
business_damage_naive, business_damage_dedup, business_damage_upsert,
missing_payments_<s>, duplicate_payments_<s>, unexpected_payments_<s>,
incorrect_payment_amounts_<s>, incorrect_order_mappings_<s>,      # each for s = naive, dedup, upsert
wal_retained_bytes_max, outbox_backlog_max, relay_batches, batch_rows_mean, batch_rows_max,
send_failed, crash_after_n, inflight_at_crash, oldest_pending_ms,
failover_at, failover_lag_bytes, failover_lag_ms, truth_source, host, shard, git_commit,
window_p50_ms, window_p99_ms, window_max_ms, window_p99_pre_ms, window_valid,
commit_ts_missing, window_negative, xid_age,
fault_end_at, observed_until,
service_recovery_s, service_recovered, synchronization_recovery_s, synchronization_recovered,
consistency_recovery_s, consistency_recovered,
throughput_ops, latency_p50_ms, latency_p99_ms, load_s, load_failed,
db_cpu_pct, db_cpu_s, cpu_window_s, committed_changes, changes_per_s, orders_per_s, db_cpu_s_per_1000_changes,
pre_host_load, pre_db_cpu_pct, pre_p99_ms, pre_flag, thermal, run_order, started_at, finished_at,
broker_partitions, delete_dirs, delete_dirs_kb, broker_wait_s, broker_shutdown, broker_reload_ms,
drain_s, drain_reason, drain_marker_ids, drain_target_lsn, drain_slot_lsn,
dump_records, dump_control_records, dump_bytes, dump_s
```

Every row carries the random seed and the pinned versions so any cycle can be replayed. `recovery_s` (2.8 and earlier) is replaced by `consistency_recovery_s`, which keeps its meaning, plus the two other recovery times; no study data uses the old column. `business_damage_<s>` keeps its name with the 2.9 definition (Section 11.2).

| Column (2.8 and 2.9) | Meaning |
|---|---|
| `truth_source` | `primary`, or `promoted_replica` after a failover (Section 11.1) |
| `window_p99_pre_ms`, `window_valid`, `commit_ts_missing`, `window_negative`, `xid_age` | Commit-time safeguards and the window baseline (Sections 5.1 and 11.2) |
| `fault_end_at`, `observed_until`, `*_recovery_s`, `*_recovered` | Recovery times with right-censoring (Section 11.2) |
| `db_cpu_s` … `db_cpu_s_per_1000_changes` | Database cost over one interval (Section 11.2) |
| business-damage classes | Section 11.2 |
| `drain_*` | CDC drain evidence (Section 9.4) |
| `dump_control_records` | Control records in the dump (DUMP definitions above) |
| `broker_partitions` | Partitions on the broker at cycle start, internal topics included. Must stay flat through a run |
| `delete_dirs`, `delete_dirs_kb` | Partition directories marked for deletion but not yet removed, and their size, at cycle start; a broker reloads them at every restart |
| `broker_wait_s` | Total time the cycle waited for the broker to serve (RESET, DRAIN, DUMP) |
| `broker_shutdown` | `clean` or `unclean` for each broker restart in the cycle, joined with `;`; empty if none |
| `broker_reload_ms` | Longest partition-log reload after a restart in the cycle |
| `dump_records`, `dump_bytes`, `dump_s` | The cycle topic's verified dump; `dump_records` includes control records |

## 14.4 Commands

```
make up                    # docker compose up the base stack
make test                  # unit tests incl. checker golden tests (C0-2)
make run VECTORS=A-1,A-4   # run selected vectors
make run-all               # full catalogue (development only)
make run-all SHARD=2/4     # this laptop's share of the catalogue (development only)
make merge                 # combine results/*.csv from all laptops into results.csv
make report                # run analysis scripts → figures/
make verify RUN=<dir>      # check a session directory (checksums, rows, statuses)
make backup                # push results/ to the private results repo
make demo                  # one crash cycle end to end
make run-must              # session "must": must-priority correctness vectors, resumable
make run-should            # session "should": should-priority correctness vectors, resumable
make run-perf              # session "perf": P-1 on its own, on a fully idle host
make pause                 # stop after the current cycle
```

Every session target runs through `deploy/keep-awake.sh`, which uses `caffeinate -i` on macOS and `systemd-inhibit` on Linux, and passes `-session <name>`. Each session writes its own directory `results/runs/<commit>/<session>/` with its own manifest, `results.csv` and `results.order.txt`, so one session can never overwrite another's order file or add rows to another's results. The analysis reads the three session files of one commit together; `make verify` refuses a directory whose rows mix commits or image versions.

## 14.5 Pre-run checklist

Before any run used for results:

| Item | Checked by |
|---|---|
| Clean git tree, `HEAD` at the study tag | Runner (refuses) |
| Docker memory at least 11 GiB | Runner (refuses) |
| On mains power: macOS `pmset -g batt` says AC; Linux has no battery under `/sys/class/power_supply`, or one reports `online` AC | Runner (refuses) |
| Host load below `PRECHECK_MAX_HOST_LOAD` | Runner (refuses) |
| Connect's internal topics present | Runner (refuses) |
| At least 20 GB free disk | Runner (refuses) |
| Same commit and image versions in every row of a session | `make verify` (refuses to mix them) |
| `make accept` passed on this commit, on the results machine | Person running it |
| Check run passed every go criterion (Section 14.7) | Person running it |
| Results machine set up as in Section 2.1; nobody else using it | Person running it |
| `host-info.txt` written to the run directory | Person running it |
| Backup configured and tested (`make backup`; dump copy to the Mac) | Person running it |

`-allow-dirty` marks a run as a trial and skips the machine checks.

**Randomised order.** Within each group of cycles that share a worker and broker configuration, the order is shuffled using the run seed, so slow drift such as heat cannot favour one pattern. C0-1 runs strictly first, then the other controls, and failover cycles last. The order is written to `<results>.order.txt` and each row's `run_order`.

**Sessions.** A run can be stopped and resumed, so the machine can be used between sessions. `make pause`, or a first Ctrl-C, finishes the current cycle and stops. A second Ctrl-C stops at once; the interrupted cycle is discarded, not recorded as failed, and reruns on resume. Resuming is the same command. Correctness vectors (`make run-must`) and performance vectors (`make run-perf`) run in separate sessions, because P-1 needs a fully idle host. A power cut or reboot is handled the same way: the interrupted cycle is discarded and the same command resumes.

**Known test-bed limitations.** Tail latency varies between identical cycles, mostly from slow COMMITs (finding F27, measured on macOS, from disk sync inside Docker's VM). On the results machine the disk sits inside WSL2's virtual disk, so F27 is re-measured in the check run and the paper reports the results machine's figure. Durability settings are left unchanged, and p99 is reported with its spread, with medians as the primary numbers. Results come from one machine, one Postgres and one Kafka version, one Kafka client (franz-go) and synthetic load. Under WSL2, the host load is the WSL2 VM's own load; Windows processes outside it are not counted, so the `pre_p99_ms` latency flag is the main guard against background activity.

**Thermal state.** On macOS each cycle records the thermal and performance warnings (`pmset -g therm`) in `thermal`, and flags the cycle if any is present; on Apple Silicon these are warning levels, and no CPU speed limit is exposed without root. On Linux the runner reads `/sys/class/thermal/thermal_zone*/temp` and records the highest in °C; WSL2 exposes no thermal zones, so `thermal` is `unavailable` on the results machine. Both are stated limitations.

**Backups.** Results go to a separate private repo (`dualwrite-results`), one folder per host. The **runner** starts `make backup` between cycles, after RECORD and before the next RESET, once at least an hour has passed since the last one, and waits for it to finish. A backup never overlaps a cycle: it hashes every result file and pushes over the network, which would otherwise compete with Postgres and Kafka for disk and CPU and contaminate latency, window and cost measurements. The time it takes is logged and excluded from cycle overhead. No cron job runs on the results machine. They are kept apart from the code because a commit in the code repo would change the hash recorded in running cycles, and the runner refuses to mix commits in one file. Topic dumps and raw files (Section 11.5) are too large for the repo (about 1 MB per cycle together, about 3.6 GB for the paper run); `make backup` leaves out `dumps/` and `raw/`, the development Mac copies them at least once a day with `rsync` over SSH into its Drive folder (`DRIVE_BACKUP_DIR`), and `SHA256SUMS` covers them.

## 14.6 Results storage

| Item | Where |
|---|---|
| Run directory | `results/runs/<commit>/<session>/` for real runs (sessions `must`, `should`, `perf`), `results/trials/<commit>/` for trials |
| Manifest | `manifest.json`: commit, tag, seed, catalogue SHA-256, spec version, host, session, arguments. A resume into a directory with a different commit, seed, session or arguments is refused |
| Records | `results.csv`, appended and fsynced one row per cycle; `results.order.txt`; `damage/` and `logs/` |
| Topic dumps | `dumps/<cycle_id>.jsonl.gz`, one per cycle, verified before the topic is deleted (14.2, DUMP) |
| Raw data | `raw/<cycle_id>/`, every cycle (Section 11.5) |
| Recomputed metrics | `results.recomputed.csv`, with `analysis_commit` (Section 11.5) |
| Machine | `host-info.txt` (Section 2.1) |
| Checksums | `SHA256SUMS`, written by `make backup`; `make verify RUN=...` checks it and the rows |
| Exclusions | `results/EXCLUDED.md` lists runs that must not be used, with the reason |

## 14.7 Paper run plan (from v2.8)

**Rule.** Every observation in the paper comes from one frozen commit, tagged `study-v4`, run on the results machine (Section 2.1), in three sessions. Metrics are those in `results.csv`, or, after a checker fix, those recomputed from the same raw data at a named analysis commit (Section 11.5). No result from the development Mac and no earlier run (v1–v3, all listed in `EXCLUDED.md`) is used.

**Scope.** Sessions `must` (2,125 cycles), `should` (1,390) and `perf` (75), with every repeat, duration and soak exactly as in `vectors.yaml`. That is 3,590 of the catalogue's 4,010 cycles. `optional` vectors (C-4, D-4, H-1 to H-4; 420 cycles) are not run; the paper lists them as future work. This keeps the three-broker topology out of the paper run, so one broker and about 7 GB of container memory suffice.

**Steps, in order.** Each step starts only when the one before has passed.

| # | Step | Where | Passes when |
|---|---|---|---|
| 1 | Set up the results machine | PC | Section 2.1 complete; `docker info` shows at least 11 GiB; `go version` is 1.27.3 |
| 2 | Candidate commit: v2.9 code and spec, clean tree, pushed | Mac | `make accept` passes on the Mac, including every 2.9 golden fixture (Section 11.4) and the tests in Section 17 |
| 3 | Pull the candidate; `make accept` | PC | Every check passes |
| 4 | Check run (below) into `results/trials/<commit>-check/`, on the clean tree with every machine check on | PC | Every go criterion below holds |
| 5 | Re-plan: recompute the budget from the check run's overhead, REBUILD, DRAIN and DUMP times | Mac | All three sessions fit before the deadline; if they do not, decide before freezing which whole `should` vectors to leave out, and record the decision here |
| 6 | Tag the candidate `study-v4`, push the tag | Mac | Tag points at the commit that passed steps 3–4 |
| 7 | `host-info.sh`, then `make run-perf`, nobody using the machine | PC | Session finishes; `make verify` passes; recomputation reproduces `results.csv`; FAILED cycles < 2% |
| 8 | `make run-must` in tmux | PC | As step 7 |
| 9 | Review the `must` results while `should` runs | Mac | No sign of a harness fault (below) |
| 10 | `make run-should`, started as soon as `must` ends | PC | As step 7 |

`perf` runs first: it is only about 3 h, it secures RQ5 early, and it runs on a freshly started machine. `should` runs last, so a delay costs `should` cycles, never `must` ones.

**Check run.** A trial on the candidate commit, about 2.5 h (1 h of it C0-1), chosen to re-test on the results machine every harness fault found so far and every measurement added in 2.9. The runner's `-dry-run` lists the exact cycle IDs:

| Cycles | Re-tests |
|---|---|
| C0-1 on P4, the full hour | F31 (Debezium's replication connection held for an hour) |
| C0-3 on P1–P5, one each | Graceful stop gives zero damage on every pattern |
| C-1 on P1 and P4, each outage length (10, 60, 300 s), one each | F32 and bug 12 (clean broker restart, flat partition count) |
| A-1 on P1, three; A-3 on P2, one | Known damage still appears on amd64 |
| E-2 on P4, one per `OFFSET_FLUSH_MS` cell | Connect restart and offset flush; CDC drain evidence after a Connect fault (Section 9.4) |
| D-1 on P1 and P4, two each; D-2 on P4, one | Time for REBUILD and failover; `truth_source`; commit times on the promoted replica |
| B-1 on P3, one | Multi-relay reordering is still measured (Section 8) |
| G-1 and G-2 on P3, one each | Per-order business damage and the consumer processing order (Sections 10 and 11.2) |
| P-1 on P3 and P4 at 1,000/s, one each | The results machine sustains the highest load rate; cost metrics over one interval |

**Go criteria for the check run.** Each one must hold; any miss is investigated, fixed, and the check run repeated on a new candidate.

1. No cycle has a status other than `OK`; no `CONTROL_FAIL`, `FAILED_*` or `INVALID_*`.
2. Every control cycle shows zero damage of every kind.
3. `broker_partitions` is the same at every cycle start after the first.
4. Every broker restart in C-1 is `clean`, with `broker_reload_ms` under 5,000.
5. Every cycle has a verified dump (DUMP definitions, Section 14.2).
6. A-1 on P1 and A-3 on P2 both show lost events, as they did on the Mac.
7. `pre_flag` is empty in at least 90% of cycles; the p99 spread is recorded for F27.
8. The runner ran `make backup` between cycles and the results repo received the trial directory; the Mac copied the dumps and `SHA256SUMS` verifies them.
9. `make verify` passes on the trial directory.
10. Every P3 cycle was drained with its marker in the topic and every P4–P5 cycle with a marker in every partition; every P4–P5 cycle recorded `drain_target_lsn` and `drain_slot_lsn`; `drain_reason` is empty everywhere.
11. `window_valid` is `true`, and `commit_ts_missing` and `window_negative` are 0, in every cycle, including both D-1 and D-2 cycles after failover.
12. G-1: naive has `duplicate_payments > 0`; dedup and upsert have 0. G-2: `missing_payments > 0` for at least one strategy. Every other business-damage class is 0 in both.
13. Every cycle with a fault has all three `*_recovered` columns filled; every A-1 cycle on P1 with lost events has `consistency_recovered = false`.
14. `committed_changes` and `db_cpu_s` are filled in both P-1 cycles, and `changes_per_s` is within 5% of 1.8 × the offered order rate.
15. Every cycle has its full `raw/<cycle_id>/` set, and `cmd/recompute` at the candidate commit reproduces every metric column of the check run's `results.csv` exactly (Section 11.5).

**Signs of a harness fault during a session.** Any of: a `CONTROL_FAIL` (the run stops by itself); `broker_partitions` rising; an `unclean` restart where the vector intends a clean one; `broker_wait_s` or overhead per cycle rising through the session; more than 2% FAILED cycles; `FAILED_DRAIN` with the same `drain_reason` recurring; `window_valid = false` in more than an isolated cycle; `xid_age` approaching its limit; a vector whose damage changes with `run_order` rather than with its cell.

**If a harness fault appears.** First decide which kind it is, and record it in `FINDINGS.md` as a harness bug either way.

- *A metric or checker bug* (a wrong definition or calculation, with correct raw data): the session keeps running. Fix it on the Mac, test it against the golden fixtures, and recompute from the raw files at the fix's commit (Section 11.5).
- *An observation bug* (fault injection, timing, load, drain, dump or raw capture): pause the session. Fix it on the Mac, take a new candidate through steps 2–6 as `study-v5`, and start every session again from zero in its own directory. Every `study-v4` row goes into `EXCLUDED.md`. A session is never patched or resumed across commits.

When unsure, treat it as an observation bug.

**During a session.** Nobody uses the PC; the tree on the PC stays at the tag (no `git pull` in `~/capstone`); `make pause` is the only way to stop, unless the machine itself fails. Progress is read from the results repo or with `tmux capture-pane`, never by running other workloads on the PC.

## 14.8 Validity rules

What makes a cycle count, and how each threat to validity is handled. `make verify` checks every rule marked *verified*; a session that fails one is not used.

| Threat | Rule | How it is enforced |
|---|---|---|
| A fault on an idle system | `INVALID_NO_LOAD` (and `INVALID_NO_CRASH`) cycles are excluded from every correctness and recovery conclusion | Analysis uses `status = OK` rows only for RQ1–RQ4; *verified*: no such row is in an OK count |
| Failed cycles hidden | Every cell reports planned, OK, `FAILED_<state>`, `INVALID_*` and `CONTROL_FAIL` counts; the paper's appendix lists them per cell, and a cell with under 80% of its planned cycles OK is reported as incomplete, never silently | Analysis table; *verified*: rows + missing = planned for every cell |
| Order effects | Order is shuffled within groups by the run seed (Section 14.5) | *Verified*: `make verify` recomputes the order from the seed and the catalogue and compares it with `results.order.txt` and every row's `run_order` |
| Wrong truth | Before a failover the truth is the primary; after it, the promoted replica | `truth_source` in every row; *verified*: `promoted_replica` exactly in the cycles with `failover_at` set |
| Contaminated performance | `perf` runs alone (Section 14.5), with no other session, backup or Mac-side work on the PC; backups never overlap a cycle (14.5) | Runner-driven backups; `pre_flag`; host load recorded per cycle |
| Unequal workload | Every pattern gets the same open-loop schedule: the same rate, mix (60% pay, 20% cancel), think times and warm-up. Seeds differ per cycle, and the schedule depends only on the seed and rate, never on the pattern | *Verified*: per cell, mean `changes_per_s` within 5% of 1.8 × offered rate, except cells whose fault blocks the order service (reported, not excluded) |
| Pseudo-replication | The cycle is the unit: events within one cycle are never treated as independent repetitions (Section 15.2) | Analysis code works on per-cycle rows only |
| Selective reporting | Hypotheses, tests and the analysis rule are fixed in this spec before the paper run (Sections 1.4 and 15) | Spec tag at `study-v4` |

# 15. Load generator (`cmd/loadgen`) and analysis

## 15.1 Load generator and figures

**Load generator:** open-loop (fixed schedule, independent of response time) so a slow system does not reduce offered load. Modes: `constant` (rate/s), `burst` (base rate with a 10× spike for N seconds). Each created order is later paid (60%) or cancelled (20%) after a random 50–500 ms, so every order has one or two versions in total (80% get one follow-up change, 20% none). It records per-request latency into an HDR-style histogram and reports p50/p99 and achieved throughput to the runner. It also reports, for every second of the cycle, the requests issued and the requests that succeeded, which `service_recovery_s` needs (Section 11.2). An order's follow-up is sent only after its create request has returned successfully, so an order's versions are committed one after the other.

**Rates.** The configured `rate_per_s` is **orders** per second (create requests). Each order brings 1.8 changes on average (one create, then a pay or cancel for 80% of orders), so the database commits about 1.8 × `rate_per_s` **changes** per second: about 180/s at the default 100 orders/s, and 1,800/s at 1,000. Cost and throughput are reported per committed change (`changes_per_s`, `db_cpu_s_per_1000_changes`); `orders_per_s` is reported beside them, and the two are never used interchangeably.

**Analysis:** Python scripts in `analysis/` read `results.csv` and produce, at minimum:
damage by pattern × fault (bars with 95% confidence intervals), a pattern × fault heatmap, knob sweeps (lines), exactly-once on/off and consumer-strategy comparisons, and inconsistency-window distributions.

**Window and cost by load, fixed before the final runs.** P3, P4 and P5's inconsistency window depends on load: the relay sleeps only when it finds nothing to send, and Debezium polls on an interval. So the window is reported at every load level in P-1, never as a single number, and the paper states that a single window figure would be misleading. Database cost is reported as CPU seconds per 1,000 committed changes, `db_cpu_s_per_1000_changes = db_cpu_s / committed_changes × 1000`, with CPU and changes measured over the same interval (Section 11.2), so patterns compare fairly across load levels. Versions 2.8 and earlier used `db_cpu_pct / (throughput_ops / 1000)`, which has the wrong unit (percent of a core per request rate, not CPU time per change), sampled CPU from the start of warm-up but throughput from its end, and counted HTTP successes rather than committed changes.

**Results that need a cause first.** A result that looks anomalous is not reported until its cause is known. Currently: P2's p99 latency at 1,000 orders/s, if the P-1 repeats confirm it.

**Loss distributions, fixed before the final runs.** For every loss metric, the analysis reports the percentage of cycles with any loss, the median, p90, maximum and a histogram, not just the mean. The same outage can lose nothing or lose everything in flight (finding F24), so a mean alone would mislead.

**Analysis rule, fixed before the final runs.** Every result is reported twice: over all cycles, and excluding cycles with a `pre_flag`. "All cycles" is the primary number. Flagged cycles are never dropped from the data. The rule is set in advance because deciding after seeing the data would bias the results.

## 15.2 Statistical reporting

Fixed before the paper run, like the rules above. Python with `numpy` and `scipy` 1.16 (pinned in `analysis/requirements.txt`): `bootstrap`, `permutation_test`, `fisher_exact`, `kruskal`, `mannwhitneyu`, `kendalltau`, and for censored recovery times `ecdf` on `CensoredData` (Kaplan–Meier) and `logrank`. Cliff's delta, Wilson and Newcombe intervals and the Holm adjustment are not in SciPy; they are short functions in `analysis/stats.py`, each with a unit test against published worked examples. The resampling seed is fixed and stated.

**Unit of analysis.** One cycle is one independent observation. A cell (vector × pattern × sweep combination) holds its repeats. Events within a cycle share the same fault, timing and system state, so they are never treated as independent repetitions: every statistic is computed from per-cycle values (a count, a rate, a percentile or a yes/no per cycle), and every resample draws whole cycles.

**Per cell, always reported:** planned cycles, valid cycles (`status = OK`, which gives *n*) and the count of each other status (Section 14.8); the share of cycles with any damage of each kind, with a Wilson 95% interval; and for each metric the median, p90, maximum and mean, as fixed in Section 15.1.

**Confidence intervals.** For a mean or median per cell, and for a difference or ratio between two cells: a percentile bootstrap over cycles, 10,000 resamples, 95%, resampling within each cell separately. With *n* < 10 the interval is reported but marked as unstable. For cells where no cycle shows the damage, the bootstrap interval collapses to zero, so the rule of three is reported instead: the true share of cycles with that damage is below 3/*n* at 95% confidence (below 10% with 30 cycles).

**Effect sizes**, with every comparison that supports a claim:

| Comparison | Effect size |
|---|---|
| Two cells, per-cycle counts or times (skewed, many zeros) | Cliff's delta, with bootstrap 95% CI; plus the difference in medians |
| Two cells, share of cycles with damage | Risk difference and risk ratio, with Wilson or Newcombe intervals |
| Trend over a sweep | Kendall's tau between level and per-cycle value |
| Costs (RQ5) | Ratio of means, with bootstrap CI |

**Significance tests.** Two-sided, α = 0.05, each named with its hypothesis in Section 1.4:

| Question | Test |
|---|---|
| Two cells, per-cycle values | Permutation test on the difference in means (10,000 permutations); Mann–Whitney U as a check |
| Several patterns in one condition | Kruskal–Wallis, then pairwise permutation tests |
| Share of cycles with damage | Fisher's exact test |
| Ordered sweep levels | Kendall's tau between level and per-cycle value, p-value by permutation |
| Recovery times with censored cycles | Kaplan–Meier curves per pattern, log-rank test; medians from the curve, and "not reached" when more than half are censored |

**Censoring.** A recovery time with `<x>_recovered = false` is right-censored at its observation time (Section 11.2). It is never dropped, never replaced by the censoring time as if it were an event, and never averaged. The share of unrecovered cycles is reported with every recovery result.

**Multiple comparisons.** Each research question is one family. Within a family, p-values are adjusted with the Holm–Bonferroni method, and both raw and adjusted values are reported. A hypothesis is called supported only when its adjusted p-value is below 0.05 **and** its effect points the predicted way. Exploratory comparisons not listed in Section 1.4 are labelled exploratory and are not adjusted into the families.

**Primary and secondary analyses.** The rule above stands: every test is run on all valid cycles (primary) and again excluding cycles with a `pre_flag` (secondary). If the two disagree on whether a hypothesis is supported, the paper reports both and does not call it supported.

# 16. Docker Compose requirements

- All services `init: true` (crash hooks, Section 7).
- Memory limits: Postgres 1 GB each, Kafka 1.5 GB per broker, Connect 1.5 GB, Go services 256 MB each. About 7 GB with one broker and about 9 GB with three (C-4). Give Docker at least 12 GB: Docker Desktop's memory setting on the Mac, `memory=13GB` in `.wslconfig` on the results machine (Section 2.1).
- Postgres replica bootstrapped with `pg_basebackup` in its entrypoint; streaming replication, asynchronous by default (synchronous for D-4).
- Two Toxiproxy instances: `toxiproxy` for Kafka and `toxiproxy-pg` (API 8475) for Postgres. Toxiproxy dials upstream inside its accept loop, so one dial to a dead address blocks every new client for about 25 s. `toxiproxy-pg` is restarted at failover and after a rebuild, which clears that without touching Kafka traffic.
- Toxiproxy proxies, grouped by client class so faults can target one side:

| Group | Proxy (listen → upstream) | Used by |
|---|---|---|
| `pg-app` | 15432 → primary:5432 | order service, relay, consumers |
| `pg-connect` | 15433 → primary:5432 | Kafka Connect |
| `pg-repl` | 15434 → primary:5432 | Replica's streaming replication, so lag can be injected (`REPL_LATENCY_MS`, D-1) |
| `kafka-app` | 1909N → broker-N:9092 (`APP` listener), one per broker | order service, relay, consumers |
| `kafka-connect` | 1919N → broker-N:9094 (`CONNECT` listener), one per broker | Kafka Connect |

- Clients connect to whatever address each broker **advertises**, so a single proxy cannot front three brokers. Each client class needs its **own broker listener and port**: `APP` on 9092 advertising `toxiproxy:1909N`, `CONNECT` on 9094 advertising `toxiproxy:1919N`. If both proxies pointed at 9092, the broker would advertise the app proxy to Connect and app-side faults would hit CDC too.
- Measurement never goes through Toxiproxy: the checker and runner read Postgres directly and reach Kafka on `INTERNAL` (9093, in-network) or `HOST` (published to the host machine as `localhost:2909N`, because the host cannot resolve `broker-N`).
- Kafka Connect uses its own internal topics (`connect-configs`, `connect-offsets`, `connect-status`), which are **not** reset per cycle; per-cycle connector names and slots keep cycles isolated.
- Broker data lives on named volumes, one set per topology (`kafka-1`; `kafka-b3-1` to `kafka-b3-3`). Without them, recreating a broker wipes Connect's internal topics and Connect floods its log (finding F0).
- Each topology has its own Connect group and internal topics, with the replication factor set explicitly (1, or 3 with suffix `-b3`), so one-broker state cannot leak into the three-broker stack. Connect starts only after every broker is healthy. The three-broker volumes are wiped together whenever that topology starts, because a quorum cannot mix old and new storage.

# 17. Testing the harness itself

| Level | What | When |
|---|---|---|
| Unit | Checker golden fixtures; event schema; relay batching logic; crash guard | Every commit |
| Integration | One P1 cycle with `after_db_commit` must report exactly the expected lost count; one P3 cycle with no faults must report zero damage | Before every overnight run |
| Controls | C0-1, C0-3 per pattern | Before core experiments |
| Checker (2.9) | Golden fixtures for business damage, the drain marker, commit-time safeguards and censored recovery (Section 11.4) | Every commit |
| Integration (2.9) | `VACUUM (FREEZE)` on a filled `orders_history` leaves every commit time unchanged (Section 5.1). The relay's query returns rows with equal `created_at` in `id` order. Two relays with a stall on one publish an order's versions out of order (the B-1 mechanism, Section 8). A CDC drain with the connector paused for 15 s does not end until the marker arrives (Section 9.4). Both consumer orders crash at their hook and leave exactly the predicted payments (Section 10) | Every commit, in `make accept` |
| Verify (2.9) | `make verify` recomputes the run order from the seed, checks `truth_source` against `failover_at`, and checks rows + missing = planned per cell (Section 14.8) | Every session |

A run is only valid if all three levels pass on the same commit. Record the git commit hash in `run_id`.

Milestones are committed through `deploy/commit-milestone.sh`, which runs `make accept` (tests, lint and every smoke test) and refuses to commit on any failure.

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
| 9 | Full catalogue | The `must`, `should` and `perf` sessions of `study-v4` complete on the results machine (Section 14.7) with < 2% FAILED cycles in each, and `make verify` passes on every session directory |
| 10 | Analysis | `make report` regenerates all figures from `results.csv` |

# 19. Conventions

- Go: standard layout, `gofmt`, `go vet`, `golangci-lint`; errors wrapped with `%w`; contexts with timeouts on every network call.
- Every tunable is an environment variable with a documented default in `internal/config`; nothing is hard-coded.
- No component reads another component's internal state; all evidence comes from Postgres, Kafka and logs.
- Results, seeds, versions and the git commit are always recorded together. A number without them is not used in the paper.

# 20. Changes in versions 1.1 to 2.9

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
| 43 | 12 | (1.9) Failover moves the name `postgres-primary` by network alias instead of updating the proxies |
| 44 | 12, 14.1 | (2.0) Network fault rule; F-4 and F-5 target the publisher's path per pattern |
| 45 | 12, 14.1, 16 | (2.0) `pg-repl` proxy and D-1 sweep of `REPL_LATENCY_MS`; lag at failover recorded |
| 46 | 8 | (2.0) `RELAY_DB_TIMEOUT_MS` |
| 47 | 11.3 | (2.0) Lost pairs carry commit time and final version |
| 48 | 16 | (2.0) Separate volumes, Connect topics and replication factors per topology |
| 49 | 12, 16 | (2.0) Separate `toxiproxy-pg` instance, restarted at failover |
| 50 | 14.2, 14.3 | (2.0) `INVALID_NO_LOAD` and `load_failed` |
| 51 | 11.2, 14.1, 14.3 | (2.1) `lost_after_ack`, and C-4's expectation rewritten around it |
| 52 | 9.3 | (2.1) Debezium retry wait recorded and reported with recovery times |
| 53 | 15 | (2.1) Loss metrics reported as distributions |
| 54 | 17 | (2.1) `make accept` and a commit script that refuses to commit on failure |
| 55 | 14.5 | (2.2) Pre-run checklist, mostly enforced by the runner |
| 56 | 14.1 | (2.2) `burst_at_s` counts from the end of warm-up; `PG_IDLE_TX_TIMEOUT_S` applied per cycle |
| 57 | 14.4, 14.5 | (2.3) `make verify`, `make backup`, `make demo`; results in a separate private repo |
| 58 | 15 | (2.4) Window reported per load level; database CPU per 1,000 changes; anomalies need a cause before reporting |
| 59 | 14.2 | (2.5) Randomised order within groups; resumable sessions; interrupted cycles discarded; thermal state per cycle |
| 60 | 14.2, 14.6 | (2.6) C0-1 strictly first; run directories with manifest and checksums; exclusions list |
| 61 | 9.3 | (2.7) Debezium heartbeats every 10 s, after C0-1 failed on P4 (F31) |
| 62 | 14.2, 14.3, 14.6 | (2.8) DUMP state: each cycle topic is dumped, verified and deleted, so the partition count stays flat; the broker is waited for in RESET, DRAIN and DUMP; columns `broker_*`, `delete_dirs*`, `dump_*` (harness bug 12, F32) |
| 63 | 1.1, 2, 2.1 | (2.8) Paper results come from one dedicated x86-64 results machine under WSL2; images need `amd64` builds too |
| 64 | 14.2, 14.5 | (2.8) Machine checks work on Linux: power, host load (`/proc/loadavg`), thermal state; `HEAD` must be at the study tag |
| 65 | 14.4, 14.6 | (2.8) One directory per session (`must`, `should`, `perf`) with its own manifest and order file; `make run-should`; session targets keep the host awake on macOS and Linux |
| 66 | 14.2 | (2.8) Run-time budget from the measured v3 overhead (15 s per cycle) |
| 67 | 14.7 | (2.8) Paper run plan: scope must + should + perf, no cuts; check run with go criteria; rule for harness faults found mid-session |
| 68 | 14.5 | (2.8) Backups: hourly `make backup` from cron; topic dumps copied daily to the Mac |
| 69 | 1.4 | (2.9) Research questions RQ1–RQ5 and hypotheses H1–H5, each with its measure and test |
| 70 | 1.1, 14.2, 14.7 | (2.9) Cycle counts corrected: 4,010 in the catalogue, 3,590 in the paper run (was "~3,000"); budget table totals added |
| 71 | 11.2, 11.4, 14.3 | (2.9) Business damage per order in five disjoint classes per strategy; the 2.8 net difference let a missing and a duplicate payment cancel. New columns; golden fixtures |
| 72 | 11.2, 14.3, 15.1 | (2.9) Database cost as CPU seconds per 1,000 committed changes over one interval; orders/s and changes/s distinguished. The 2.8 formula mixed units and intervals |
| 73 | 11.1, 11.2, 14.3, 15.2 | (2.9) `recovery_s` split into service, synchronization and consistency recovery, each with a `*_recovered` flag; unrecovered cycles right-censored, never given an invented time |
| 74 | 5.1, 14.2, 14.3 | (2.9) Commit-time safeguards: read in the same cycle, `xid_age` limit, `track_commit_timestamp` checked on both servers, behaviour after failover stated, missing or negative values excluded and counted with `window_valid`; freeze test |
| 75 | 10 | (2.9) Consumer processing order written out step by step for `effect_first` and `offset_first`; hook names defined against the Kafka offset commit; dedup's single transaction stated as a guarantee. Code unchanged: it already did this |
| 76 | 9.4, 14.2, 14.3 | (2.9) CDC drain needs positive evidence: connector and task state, active slot, a drain marker committed after load stops, then stable offsets; P2 waits for scheduled retries; `FAILED_DRAIN` reasons |
| 77 | 5.1, 8 | (2.9) Relay reads `ORDER BY created_at, id`, with a matching index; per-order ordering stated as guaranteed for one relay only, and not for two or more (B-1, B-5, H2c) |
| 78 | 14.2, 14.3 | (2.9) Dump definitions: franz-go's `KeepControlRecords` with read-uncommitted exposes control batches; `dump_records` includes them; `dump_control_records`; consecutive-offset check |
| 79 | 15.2 | (2.9) Statistical reporting: the cycle as the unit, bootstrap CIs over cycles, valid *n* per cell, effect sizes, named tests, Kaplan–Meier for censored recovery, Holm–Bonferroni per research question |
| 80 | 14.5, 14.8 | (2.9) Validity rules with `make verify` checks; `truth_source` column; backups run by the runner between cycles instead of cron, so they never overlap a measurement |
| 81 | 5.3 | (2.9) Topic text updated: topics are deleted per cycle (since 2.8), not in bulk at the end |
| 82 | 9.4, 14.2 | (2.9, review) One drain marker per partition for P4 and P5: Kafka orders records only within a partition, so a single marker proved nothing about the others |
| 83 | 1.4, 15.2 | (2.9, review) Tests limited to what the pinned SciPy 1.16 provides, plus named helper functions; Jonckheere–Terpstra replaced by Kendall's tau with a permutation p-value |
| 84 | 1.4, 8 | (2.9, review) H2a uses C-1 only (C-3r has no P1); H1 names A-6 as a random crash; B-2 added as a multi-relay vector; pilot counts for one-relay P3 stated |
| 85 | 11.2 | (2.9, review) Observation limits of the three recovery times stated |
| 86 | 11.3, 11.5, 14.5, 14.6 | (2.9) Raw data saved for every cycle (truth with commit times, outbox, consumer tables, load series, markers, timeline); `cmd/recompute` must reproduce `results.csv` exactly; checker bugs fixed by recomputation at a named analysis commit, observation bugs by a new run |
| 87 | 14.7 | (2.9) Session order perf, must, should; step 5 decides which whole `should` vectors to drop if the deadline requires it; go criterion 15 (recomputation) |
