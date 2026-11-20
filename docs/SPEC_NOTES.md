# Spec notes

`docs/SPEC.md` is the single master copy, changed only on instruction. This file records open questions about it.

## Open issues

| # | Section | Problem | What the code does | Status |
|---|---|---|---|---|
| 1 | 6.4, B-3 | Idempotent records in flight may not time out. Taken from the franz-go source, not yet seen under a real fault. | B-3 sweeps `KAFKA_IDEMPOTENT`. | Confirm at M7 |
| 2 | 6.4 | `KAFKA_REQUEST_OVERHEAD_MS = 1000` should make hung connections visible within F-2 and F-3's 10 s fault. | Set in the publisher. | Confirm at M7 |
| 5 | A-3, C-3 | P2's retries start from `SEND_FAILED`, which appeared only after Kafka returned. If so, A-3's crash point cannot fire during the outage, and C-3's crash finds no retry queue. | Needs a decision at M6. | Open |

Measured findings are in `docs/FINDINGS.md`.

## Spec 2.8 and 2.9: code not yet matching the spec

The spec leads; everything below must land in the candidate commit before step 2 of SPEC 14.7. Items from 2.8 first, then 2.9.

| Section | Spec says | Code today |
|---|---|---|
| 14.5 | Power check works on Linux | `strictChecks` calls `pmset`, so the runner refuses to start on Linux |
| 14.2 | Host load from `/proc/loadavg` on Linux | `hostLoad` calls `sysctl` and silently returns 0 on Linux |
| 14.5 | Thermal state on Linux, `unavailable` under WSL2 | `thermalState` calls `pmset` and records `unknown` |
| 14.5 | `HEAD` at the study tag | Only a clean tree is checked |
| 14.4, 14.6 | `-session`, one directory per session, manifest records the session and refuses other arguments | All sessions share `runs/<commit>/`; each overwrites `results.order.txt`; the manifest checks only commit and seed |
| 14.4 | `make run-should`; `deploy/keep-awake.sh` | No `run-should`; `run-must` and `run-perf` call `caffeinate`, which is missing on Linux |
| 14.4 | `make verify RUN=<dir>` works for every session | Hard-codes `-priority must`; uses `shasum`, which needs `perl` on Ubuntu |
| 2.1 | `deploy/host-info.sh` | Missing |
| 14.5 | Dumps copied to the Mac daily | No script; `backup.sh` warns when `DRIVE_BACKUP_DIR` is unset |
| 11.2 | Business damage in five per-order classes per strategy | `checker.go:213` computes `max(rows−paid, paid−rows)`; `ReadPayments` returns only row counts |
| 11.2, 15.1 | `db_cpu_s`, `committed_changes`, one interval from warm-up end to load stop | `runner.go:365` samples CPU before warm-up; only `db_cpu_pct` is recorded |
| 11.2 | Three recovery times with `*_recovered` and censoring | Only `RecoveryS`, left empty when not recovered; no `fault_end_at` or `observed_until` columns |
| 15.1 | Load generator reports issued and succeeded requests per second | Only successful requests are sampled; failures are counted, not timed |
| 5.1, 14.2 | `xid_age`, `track_commit_timestamp` on both servers, `commit_ts_missing`, `window_negative`, `window_valid` | `ReadDB` fails the whole check on one NULL commit time; no negative-window check |
| 9.4, 14.2 | Drain marker, connector/task state, slot `active`, P2 retry wait, `drain_*` columns | Drain ends on stable offsets (and no unsent rows for P3) only |
| 8, 5.1 | `ORDER BY created_at, id`; index `(created_at, id)` | `store.go:108` orders by `created_at` only; index on `created_at` |
| 14.2 | `dump_control_records`; consecutive-offset check | Dump counts records per partition; no control count, no gap check |
| 14.3 | `truth_source` | Not recorded |
| 14.5 | Runner starts `make backup` between cycles | Backup is a separate command, run by hand or cron |
| 14.8, 17 | `make verify` recomputes order, checks `truth_source` and per-cell counts | Verify checks checksums, commits, versions and missing cycles |
| 11.4, 17 | 2.9 golden fixtures and integration tests | Not written |
| 11.5 | `raw/<cycle_id>/` for every cycle: truth, outbox, consumer, load, markers, timeline | Only the topic dump; damage JSON and logs only for damaged or failed cycles |
| 11.5 | `cmd/recompute`, reproducing `results.csv` exactly at the run commit | Missing; the checker reads live Postgres and Kafka, so it needs a raw-file input path |
| 14.5 | `make backup` leaves out `raw/` as well as `dumps/` | Leaves out `dumps/` only |
| 14.7 | `make run-perf` before `make run-must` | Order is a person's step, nothing to change in code; `run-perf` must not need `must` to have run |

Checked and already matching 2.9, no change needed: the consumer runs both processing orders exactly as SPEC 10 lists them, with dedup in one transaction (`store.go` `PayDedup`, `cmd/consumer/main.go` `handle`); the dump client already uses `ReadUncommitted()` and `KeepControlRecords()` (`broker.go`); the load generator sends a follow-up only after its create succeeded (`load.go` `order`).

## Not yet in the runner

The `recovery_s` column is empty until Milestone 7. The runner refuses, by name, any vector that needs a part not built yet.

## To do at Milestone 7

- Cap broker and Connect heaps (for example 512 MB per broker) before raising Docker memory. Three KRaft brokers may then fit in 9–10 GB instead of 12 GB. Measure, then update Section 16.

## Implementation decisions not stated in the spec

- **Runner and checker bypass Toxiproxy** using host ports (Postgres 55432, Kafka 29091).
- **Kafka topic auto-creation is off**, so a wrong topic name fails instead of silently creating a topic with the wrong partition count.
- **Physical slot `replica_slot`** keeps WAL for the replica during long runs.

## Verified client defaults (spec Section 2)

franz-go 1.22.2, read from source: acks=all, idempotence on, linger 10 ms, snappy compression, 1 in-flight request per broker, unlimited retries, no delivery timeout, 10 s produce timeout, 10 s request overhead. The publisher sets acks, idempotence, linger, compression and all timeouts explicitly.

Still to verify: Kafka Connect's producer defaults and Debezium exactly-once support for Postgres (M6).

## Pinned versions

Postgres 16.15, Kafka 4.3.2, Debezium 3.7.0.Final, Toxiproxy 2.13.0. All run natively: arm64 on the development Macs, amd64 on the results machine (SPEC 2.1). Go services: `golang:1.27.3-alpine3.24` build, `alpine:3.24.4` runtime; `go.mod` pins pgx 5.12.0 and franz-go 1.22.2.
