# Fault-injection study of database–Kafka consistency patterns

Measures how five ways of keeping Postgres and Kafka in sync behave under failures: lost, ghost, duplicate and out-of-order events, the inconsistency window, business damage and performance cost.

| Pattern | Who publishes to Kafka |
|---|---|
| P1 | Order service, after commit (naive dual write) |
| P2 | Order service, with in-memory retry |
| P3 | Polling relay on a transactional outbox |
| P4 | Debezium on a transactional outbox |
| P5 | Debezium on the business table |

## Requirements

- Docker Desktop with about 12 GB of memory (needed for the three-broker cycles)
- Go 1.27 or newer
- `golangci-lint`, only for `make accept`
- Python 3.12 and `uv`, only for analysis

Built and tested on Apple Silicon. All images have arm64 builds.

## Run one cycle

```
make demo
```

This starts the stack, crashes the order service right after a database commit, and prints the result. Expect `status` OK and at least 1 lost event. The first run downloads the images, which takes a few minutes.

## Common commands

| Command | What it does |
|---|---|
| `make up` | Start the stack |
| `make reset` | Stop the stack and delete all data |
| `make test` | Unit tests, including the checker's golden tests |
| `make accept` | Tests, lint and every smoke test |
| `go run ./cmd/runner -dry-run` | Show the plan and any refused vectors |
| `make run VECTORS=A-1 ARGS="-patterns P1"` | Run selected vectors |
| `make run-all SHARD=2/4` | Run this laptop's share of the catalogue |
| `make merge` | Combine results from several laptops |
| `make verify FILE=results/results.csv` | Check a results file before using it |
| `make backup` | Push `results/` to the private results repo |

Add `ARGS="-allow-dirty"` for a trial on uncommitted code. Trial rows are marked `-dirty` and are not paper results.

## Where things are

| Path | Contents |
|---|---|
| `docs/SPEC.md` | The specification, which is the master reference |
| `docs/FINDINGS.md` | Findings, surprises and harness bugs, each with its evidence |
| `experiments/vectors.yaml` | The test catalogue |
| `experiments/trials/` | Trial data, not study results |
| `cmd/`, `internal/` | The Go code |

Before a run used for results, follow the pre-run checklist in Section 14.5 of the spec.
