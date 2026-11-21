# Spec notes

`docs/SPEC.md` is the single master copy, changed only on instruction. This file records open questions about it.

## Open issues

| # | Section | Problem | What the code does | Status |
|---|---|---|---|---|
| 1 | 6.4, B-3 | Idempotent records in flight may not time out. Taken from the franz-go source, not yet seen under a real fault. | B-3 sweeps `KAFKA_IDEMPOTENT`. | Confirm at M7 |
| 2 | 6.4 | `KAFKA_REQUEST_OVERHEAD_MS = 1000` should make hung connections visible within F-2 and F-3's 10 s fault. | Set in the publisher. | Confirm at M7 |
| 5 | A-3, C-3 | P2's retries start from `SEND_FAILED`, which appeared only after Kafka returned. If so, A-3's crash point cannot fire during the outage, and C-3's crash finds no retry queue. | Needs a decision at M6. | Open |

Measured findings are in `docs/FINDINGS.md`.

## Spec 2.8 and 2.9: state of the code

Done and unit-tested (commits on branch `spec-2.9`): every item of the former table. Business damage per order, cost over one interval, the three recovery times with censoring, commit-time safeguards, drain markers and CDC evidence, `GET /pending`, relay `ORDER BY created_at, id`, dump gap check and control count, `truth_source`, raw files and `cmd/recompute`, runner-driven backups, sessions with manifests, the check-run preset, `verify` against the manifest, Linux host checks, `keep-awake.sh`, `host-info.sh`.

Still to do before the candidate commit (SPEC 14.7, step 2):

| Section | Item | Needs |
|---|---|---|
| 17 | Integration tests against the running stack: `VACUUM (FREEZE)` keeps commit times; relay returns equal `created_at` rows in `id` order; two relays with a stall reorder; a CDC drain with the connector paused does not end until the marker arrives; both consumer orders crash at their hook with the predicted payments | Docker on the Mac; `deploy/smoke-v29.sh` in `make accept` |
| 14.2 | One real cycle of each pattern through the new runner, with `make recompute` on its trial directory | Docker on the Mac |
| 15.2 | Analysis code (`analysis/stats.py` and its tests) | Can be written while the study runs |

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
