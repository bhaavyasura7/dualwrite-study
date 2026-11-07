# Spec notes

`SPEC.md` (v1.1) and `experiments/vectors.yaml` are kept verbatim. This file records where the implementation differs from them, and why. The 12 issues raised against v1.0 are resolved in v1.1 (Section 20).

## Open issues in v1.5

| # | Section | Problem | What the code does | Status |
|---|---|---|---|---|
| 1 | 6.4, B-3 | Idempotent records in flight may not time out. Taken from the franz-go source, not yet seen under a real fault. | B-3 sweeps `KAFKA_IDEMPOTENT`. | Confirm at M7 |
| 2 | 6.4 | `KAFKA_REQUEST_OVERHEAD_MS = 1000` should make hung connections visible within F-2 and F-3's 10 s fault. | Set in the publisher. | Confirm at M7 |
| 5 | A-3, C-3 | P2's retries start from `SEND_FAILED`, which appeared only after Kafka returned. If so, A-3's crash point cannot fire during the outage, and C-3's crash finds no retry queue. | Needs a decision at M6. | Open |
| 6 | C-1 | franz-go buffers 50,000 records by default. A 300 s outage at about 178 events/s needs 53,400, so `Produce` would block. | Set `MaxBufferedRecords` explicitly at M6. | M6 |

Measured findings are in `docs/FINDINGS.md`.

## Not yet in the runner

The `recovery_s` and `wal_retained_bytes_max` columns are empty until Milestones 6 and 7. The runner refuses, by name, any vector that needs a part not built yet.

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

Postgres 16.15, Kafka 4.3.2, Debezium 3.7.0.Final, Toxiproxy 2.13.0. All run as native arm64. Go services: `golang:1.27.3-alpine3.24` build, `alpine:3.24.4` runtime; `go.mod` pins pgx 5.12.0 and franz-go 1.22.2.
