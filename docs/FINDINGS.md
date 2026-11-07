# Findings log

Running record for the paper. Every entry names its evidence, so a claim can be traced to data.

**Status of each entry**
- **Trial:** manual or dirty-tree run, used to build and debug the harness. Good for explaining behaviour and designing tests. Not a study result; do not report these numbers as results.
- **Study:** produced by the runner on a clean commit with seeds and versions recorded. None yet.

Trial data lives in `experiments/trials/`, the scripts that analysed it in `analysis/diagnostics/`.

## Stack for all entries below

Postgres 16.15, Kafka 4.3.2, Debezium 3.7.0.Final, Toxiproxy 2.13.0, franz-go 1.22.2, pgx 5.12.0, Go 1.27. Apple Silicon Mac, 16 GB, Docker VM 7.7 GB. P1, 100 orders/s (about 178 changes/s), `acks=all`, linger 5 ms, delivery timeout 5 s, request timeout 3 s, overhead 1 s.

## F0. Harness incident: Kafka Connect log storm (read this first)

Recreating the broker wiped Connect's internal topics, because broker data is not on a volume. Connect then looped on "unknown topic" and wrote about 20 MB/s of warnings: 61.8 GB in under 90 minutes, filling the Mac to 97%. It began at 01:05:57 UTC and ended when Connect was recreated at about 02:27 UTC.

Effects: seconds-long stalls that fenced the broker (4 `FAILED_RESET` cycles) and added latency to everything on the disk. All timing data before the fix is unreliable. The runner now refuses to start if Connect's topics are missing or the disk has under 20 GB free, and RESET waits for a fenced broker.

Methods point: a background process on shared disk changed p99 latency about 15-fold without producing a single error in the system under test.

Evidence: `experiments/trials/2026-11-07/README.md`, `connect-log-storm/log-sizes.txt`.

## F1. The delivery timeout did not fire during a Kafka outage (Trial)

An 8 s cut of the `kafka-app` proxy under load produced no `SEND_FAILED` during the cut, with idempotence on or off. Records failed in one burst within about 7 ms of the connection returning.

| | Idempotence on | Idempotence off |
|---|---|---|
| `SEND_FAILED` during the cut | 0 | 0 |
| Failed at reconnect, clean system | 1,521 | 1,501 |
| Slowest delivery, clean system | 8.65 s | 0.016 s |
| Failed at reconnect, during log storm | 1,554 | 1,560 |
| Slowest delivery, during log storm | 8.79 s | 0.19 s |

The result held on both runs, with and without the storm.

Evidence: `experiments/trials/2026-11-07/clean-rerun/`, `outage-idempotence-{on,off}/`, `analysis/diagnostics/analyse.py`, `more.py`.
Limits: two runs per setting, one outage type. Must be repeated with a TCP reset and with a broker stop before it is generalised.
Consistent with the franz-go docs: the timeout is evaluated before writing a request or after a produce response.

## F2. The 8.8 s delivery came from idempotence, not a long request (Trial)

Our first hypothesis was one long in-flight produce request. The trace refuted it: the request ended at once with EOF when the proxy was cut. With idempotence on, records of unknown outcome were held and delivered after reconnecting; with it off, they failed instead (see F1).

Evidence: as F1. `KAFKA_TRACE_ATTEMPTS=true` logs `PRODUCE_REQUEST`.

## F3. Dual write loses events on a Kafka outage, and the counts reconcile exactly (Trial)

8 s outage at 100 orders/s: 1,072 lost of 4,470 committed versions, with 1,072 `SEND_FAILED` markers, 0 ghosts and 0 duplicates. Lost equals failed sends exactly, from two independent sources.

Evidence: `make smoke-checker`, output under `experiments/trials/2026-11-07/smoke/`.

## F4. Crash points give exact, predicted damage (Trial)

Sequential load, crash on the 50th order, so nothing else is in flight:

| Crash point | Send mode | Committed | Lost | Hooked lost | `lost_after_send` |
|---|---|---|---|---|---|
| `after_db_commit` | async | 60 | 1 | 1 | 0 |
| `before_db_commit` | async | 59 | 0 | 0 | 0 |
| `after_send_before_flush` | async | 60 | 1 | 1 | 1 |
| `after_send_before_flush` | sync | 60 | 0 | 0 | 0 |

Evidence: `make smoke-crash`; output under `experiments/trials/2026-11-07/smoke/`.

## F5. Crashing during start-up inflates collateral loss about tenfold (Trial, during storm; repeat before use)

With `CRASH_AFTER_N = 50` and no arming, the crash fired about 0.35 s into load, with 15–22 records unacknowledged (oldest 94–150 ms) and collateral loss between 0 and 19. Arming after warm-up gave 0–1 in flight and 0–1 collateral loss in the first cycles. Normal send-to-ack time is about 6 ms at p50.

Evidence: `analysis/diagnostics/early.py`, `inflight.py`; `experiments/trials/2026-11-07/runner-trial/trial.csv`.
Methods point: a crash test must arm after warm-up, or it measures start-up rather than steady state.

## F6. The latency stalls were the log storm (Trial, resolved)

During the storm, P1 showed p50 about 1.7 ms and p99 about 180–260 ms, with irregular stalls up to 750 ms. After fixing it, the same test gave p50 0.9 ms, p99 13.6 ms, max 62 ms. `/healthz` p99 3.3 ms. Neither Postgres durability settings nor `synchronous_commit` needed changing.

Evidence: `experiments/trials/2026-11-07/clean-rerun/` against `latency/`, `analysis/diagnostics/cmp.py`.

## F7. Graceful stop under load causes no damage on P1 (Trial, during storm)

Three runs, 5 s stop during load: 0 lost, 0 ghosts, 0 duplicates, exit code 0. About 600 requests per run were refused while the service was down and never committed.

Evidence: `experiments/trials/2026-11-07/graceful-stop-p1/`.

## F8. At steady state, collateral loss is small and tracks what was in flight (Trial)

A-1 on P1, 30 cycles, clean system, crash armed after warm-up at a random point: the hooked event was lost in all 30, with 0 ghosts and 0 duplicates. Collateral loss had mean 0.9, median 0.5, max 10. Correlation between collateral loss and `inflight_at_crash` was 0.97. The largest case (r17) had 10 in flight and 10 collateral lost.

`inflight_at_crash` slightly undercounts: it cannot see requests that committed but had not reached `SENT_CALLED`. That applied to 3 events in 30 cycles.

A-8: async send lost 1–3 events per crash; sync send lost 0 in 3 cycles. A-2: hooked event never lost; 0–1 collateral.

Evidence: `experiments/trials/2026-11-07/m5-acceptance/`, `analysis/diagnostics/m5_summary.py`.
Limits: P1 only, and the run was on uncommitted code.

## F9. Measured per-cycle overhead is about 12 s, not 75 s (Trial)

Mean 11.6 s (range 10.0–12.5) across 39 cycles of A-1, A-2 and A-8, of which 10 s is the drain's quiet period. At 12 s the catalogue is about 77 hours in total and 41 hours for must-run vectors. This is P1 only: P4 and P5 add connector creation, and failover vectors add a rebuild.

## F10. A few cycles show unexplained latency spikes (Trial, open)

4 of 39 cycles had p99 above 30 ms against a median of 6.4 ms: r11 (200 ms), r14 (300 ms), r18 (127 ms), A-2 r03 (79 ms). r11 and r14 overlapped a lint run on the same Mac. r18 and A-2 r03 did not clearly do so. Keep the machine idle during P-1, and look at this again in Milestone 7.

## Hypotheses to keep and test

Record these as written, then mark them supported or refuted. Do not reword them after the fact.

- **A-3:** "Every event waiting in the in-memory retry queue is lost; retries do not fix dual write." At risk from F1: the retry path may not run during an outage.
- **C-3:** "P2 loses its queued retries; the P3 outbox survives the publisher crash." Same risk.
- **B-3:** Idempotence on: in-flight records cannot time out. Off: records time out and P2 duplicates them.

## Design decisions the paper should explain

- `out_of_order` counts first deliveries only; `stale_redelivery` counts repeats below a newer version (spec 11.2).
- Topics use `LogAppendTime`, so the window uses the broker's clock (spec 5.3).
- Crash points are armed after warm-up at a random point (spec 7).
- C-4 runs both cells with idempotence off, so only `acks` differs.
