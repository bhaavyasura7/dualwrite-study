# Spec notes

`SPEC.md` (v1.1) and `experiments/vectors.yaml` are kept verbatim. This file records where the implementation differs from them, and why. The 12 issues raised against v1.0 are resolved in v1.1 (Section 20).

## Open issues in v1.3

| # | Section | Problem | What the code does | Status |
|---|---|---|---|---|
| 1 | 6.4, B-3 | Idempotent records in flight may not time out. Taken from the franz-go source, not yet seen under a real fault. | B-3 sweeps `KAFKA_IDEMPOTENT`. | Confirm at M7 |
| 2 | 6.4 | `KAFKA_REQUEST_OVERHEAD_MS = 1000` should make hung connections visible within F-2 and F-3's 10 s fault. | Set in the publisher. | Confirm at M7 |
| 3 | 15, P-1 | Order latency has p99 around 200 ms from irregular stalls on the database path. See measured findings. | Nothing yet. Test `synchronous_commit=off` to confirm before trusting P-1 latency. | M5 |
| 4 | 7, 14.1 | `CRASH_AFTER_N = 50` fires about 0.35 s into load (about 178 changes/s), far inside the 10 s warm-up, so the crash is not in steady state. | Needs a decision. | Open |
| 5 | A-3, C-3 | P2's retries start from `SEND_FAILED`, which appeared only after Kafka returned. If so, A-3's crash point cannot fire during the outage, and C-3's crash finds no retry queue. | Needs a decision at M6. | Open |
| 6 | C-1 | franz-go buffers 50,000 records by default. A 300 s outage at about 178 events/s needs 53,400, so `Produce` would block. | Set `MaxBufferedRecords` explicitly at M6. | M6 |

## Measured findings (single trial runs, P1, not study results)

- **The delivery timeout did not fire during an 8 s Kafka cut.** In two runs, one with idempotence on and one off, there was no `SEND_FAILED` during the cut. About 1,550 records failed in a burst within 7 ms when the connection returned. Consistent with the franz-go note that the timeout is only evaluated around writing a request.
- **Idempotence explains the 8.8 s delivery.** No long in-flight request exists: the trace shows the request ended with EOF at the cut. With idempotence on, 14 records of unknown outcome were kept and delivered after reconnecting (window max 8.8 s). With it off, the same records failed (window max 0.19 s).
- **Latency stalls are on the database path, cause not proven.** They are irregular (not periodic), not concentrated at the start, and decay as queued requests drain. `/healthz` stayed fast during them (max 39 ms against 750 ms for orders) and Kafka produce requests had p99 5 ms. Disk sync in the Docker VM is the leading suspect.

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
