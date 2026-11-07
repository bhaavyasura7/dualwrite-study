# Spec notes

`SPEC.md` (v1.1) and `experiments/vectors.yaml` are kept verbatim. This file records where the implementation differs from them, and why. The 12 issues raised against v1.0 are resolved in v1.1 (Section 20).

## Open issues in v1.1

| # | Section | Problem | What the code does | Status |
|---|---|---|---|---|
| 1 | 16 | Both Kafka proxy groups are listed with upstream `broker-N:9092`. A broker answers metadata with the advertised address of the listener the client arrived on, so Connect would be redirected to the app proxy and app-side faults would hit CDC too. | `CONNECT` listener on port 9094; `kafka-connect-bN` targets it. Isolation is checked in `make smoke`. | Done |
| 2 | 16 | `INTERNAL://broker-N:9093` cannot be resolved from the Mac, where the runner and checker run. | Extra `HOST://localhost:29093` listener for them. `INTERNAL` is used between brokers. | Done |
| 3 | 9.3 | The connector template still uses port 15432 (`pg-app`); Section 16 assigns Connect to `pg-connect` on 15433. | Will use 15433. | M6 |
| 4 | 16 | The checker is listed under `pg-app`, but Section 16 also says faults must never affect measurement. | Checker will use the direct host port 55432. | M3 |
| 5 | 18, 14.2 | Milestone 9 says must-run vectors finish "overnight", but the budget is about 80 hours. | Needs a decision before M9. | Open |

## Implementation decisions not stated in the spec

- **Runner and checker bypass Toxiproxy** using host ports (Postgres 55432, Kafka 29093).
- **Kafka topic auto-creation is off**, so a wrong topic name fails instead of silently creating a topic with the wrong partition count.
- **Physical slot `replica_slot`** keeps WAL for the replica during long runs.

## Pinned versions

Postgres 16.15, Kafka 4.3.2, Debezium 3.7.0.Final, Toxiproxy 2.13.0. All run as native arm64.
