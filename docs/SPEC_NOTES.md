# Spec notes

`SPEC.md` (v1.1) and `experiments/vectors.yaml` are kept verbatim. This file records where the implementation differs from them, and why. The 12 issues raised against v1.0 are resolved in v1.1 (Section 20).

## Open issues in v1.2

| # | Section | Problem | What the code does | Status |
|---|---|---|---|---|
| 1 | 6.4, B-3 | Idempotent records in flight may not time out. Taken from the franz-go source, not yet seen under a real fault. | B-3 sweeps `KAFKA_IDEMPOTENT`. | Confirm at M7 |
| 2 | 6.4 | `KAFKA_REQUEST_OVERHEAD_MS = 1000` should make hung connections visible within F-2 and F-3's 10 s fault. | Set in the publisher. | Confirm at M7 |
| 3 | 5.3, 11.2 | The window metric needs a broker timestamp, but Kafka topics default to `CreateTime`, the producer's clock. A record delayed by retries would keep its early timestamp, so the window would be understated. | Cycle topics must set `message.timestamp.type=LogAppendTime`. The checker refuses any other topic. The runner must create topics this way. | Add to spec |
| 4 | 11.2 | By the literal definition, a redelivered duplicate that arrives after a later version also counts as out of order. A-4 and A-7 republish whole batches, so `out_of_order` would rise from duplicates alone and blur the ordering result in B-1 and B-5. | Implemented literally; see `triple_duplicate.json`. | Needs a decision |
| 5 | 15, P-1 | The clean cycle had p50 latency 2 ms but p99 193 ms. Cause unknown; the first requests after start are a suspect. Load has no warm-up exclusion yet. | Nothing yet. Investigate before trusting P-1 latency numbers. | M5 |

## Implementation decisions not stated in the spec

- **Runner and checker bypass Toxiproxy** using host ports (Postgres 55432, Kafka 29091).
- **Kafka topic auto-creation is off**, so a wrong topic name fails instead of silently creating a topic with the wrong partition count.
- **Physical slot `replica_slot`** keeps WAL for the replica during long runs.

## Verified client defaults (spec Section 2)

franz-go 1.22.2, read from source: acks=all, idempotence on, linger 10 ms, snappy compression, 1 in-flight request per broker, unlimited retries, no delivery timeout, 10 s produce timeout, 10 s request overhead. The publisher sets acks, idempotence, linger, compression and all timeouts explicitly.

Still to verify: Kafka Connect's producer defaults and Debezium exactly-once support for Postgres (M6).

## Pinned versions

Postgres 16.15, Kafka 4.3.2, Debezium 3.7.0.Final, Toxiproxy 2.13.0. All run as native arm64. Go services: `golang:1.27.3-alpine3.24` build, `alpine:3.24.4` runtime; `go.mod` pins pgx 5.12.0 and franz-go 1.22.2.
