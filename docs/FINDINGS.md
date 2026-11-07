# Findings log

Running record for the paper. Every entry names its evidence, so a claim can be traced to data.

**Status of each entry**
- **Trial:** manual or dirty-tree run, used to build and debug the harness. Good for explaining behaviour and designing tests. Not a study result; do not report these numbers as results.
- **Study:** produced by the runner on a clean commit with seeds and versions recorded. None yet.

Trial data lives in `experiments/trials/`, the scripts that analysed it in `analysis/diagnostics/`.

## Surprises log

Each unexpected result, kept for the paper's findings. Status: **confirmed** means the cause was shown by a direct test, **likely** means the evidence fits but a test is missing, **open** means the cause is unknown.

| # | Surprise | Suspected cause | How checked | Status | Entry |
|---|---|---|---|---|---|
| 1 | Delivery timeout did not fire during a proxy cut or broker stop | Timeout is only evaluated around writing a request | Outage matrix over 4 outage types | Likely; holds for cut and stop, not for resets | F1, F11 |
| 2 | 8.8 s delivery with a 5 s timeout | Idempotence holds records of unknown outcome. Our first guess, a long in-flight request, was wrong | Trace, then idempotence on and off | Confirmed in 2 runs per setting | F2 |
| 3 | P2 lost nothing in short outages | Retries run after Kafka returns. Our guess that they never run was wrong | Outage matrix | Confirmed for 8 s outages | F12 |
| 4 | Retries reorder events (156–500 per outage) | A retried older version lands after a newer one | Outage matrix | Likely; H-REORDER will test it | F12 |
| 5 | Same 8 s cut lost 0 or about 1,500 | Loss is decided per partition, in one of three modes; what picks the mode is inside franz-go | 20 repeats by partition; in-flight-at-cut tested and ruled out | Mechanism partly confirmed; root cause open | F24, F28 |
| 6 | Configured batch size hardly matters above about 100 | Effective batch follows arrivals | Poll-interval sweep | Likely; H-BATCH will test it | F15 |
| 7 | CDC window of about 530 ms | `poll.interval.ms`; the maximum of about twice the interval suggests two polling stages | Poll 100 against 500 | Proportionality confirmed; two stages inferred | F14 |
| 8 | About 1,900 lost in some CDC failovers | Debezium's 10 s retry wait delays connector deletion | Connect logs: 3 waits in 3 affected cycles | Confirmed | F19 |
| 9 | Those cycles also lost 12–84 events from before failover | Events Debezium had read but not sent are dropped on a retry restart, and after failover the slot is gone, so they cannot be re-read | Retry path without failover: 20 of 20 cycles, 0 lost; raw topic check | Confirmed | F29 |
| 10 | Snapshot recovery still loses events | A snapshot holds only latest state | All 851 lost events were intermediate versions | Confirmed | F19 |
| 11 | No ghosts in D-1 at first | Replication lag near zero at this load | 200 ms of injected lag gave 16–23 ghosts | Confirmed | F20 |
| 12 | Pre-cycle latency blips; noisy p99 in repeats | Slow COMMIT (disk sync in Docker's VM) | Slow-statement log; checkpoints and drift ruled out | Likely; reported as a test-bed limitation | F27 |
| 14 | P3's window shrank from 244 ms at 100/s to 9 ms at 1,000/s | The relay sleeps only when it finds nothing to send | Two load levels | Likely | F26 |
| 15 | P4 and P5's window also shrank under load (about 530 to about 320 ms) | Debezium's polling interval | Two load levels | Open | F26 |
| 16 | P2's p99 latency 432 ms at 1,000/s, against 64–128 ms for the others | Not known | One 60 s run | Open; must be explained before reporting | F26 |
| 13 | G-4 showed no naive duplicates | Per-record commits leave at most one event uncommitted | One cycle only | Open | F25 |

## Harness bugs

Bugs in our own test setup, for the method section. None reached study data, because none exists yet.

| # | Bug | What it would have distorted | How found |
|---|---|---|---|
| 1 | Broker recreated without a volume, so Connect's topics vanished and it logged 20 MB/s | All timing data; p99 about 15× too high; disk full | 4 failed cycles, then disk inspection |
| 2 | Crash fired at 0.35 s, inside warm-up | Collateral loss about 10× too high | In-flight logs |
| 3 | Connect flush setting overwritten by the image's entrypoint | Every E-2 and E-3 sweep cell would have run at 60 s | Duplicates far above expectation |
| 4 | Connector names reused across attempts | Retried cycles fail to start | `FAILED_START`, Connect logs |
| 5 | Toxiproxy's accept loop blocked by a dial to a dead address | Failover downtime inflated; cycles ran idle yet reported OK | Event counts too low |
| 6 | 60 s relay timeout against the order service's 5 s | P3 recovery about 100× too slow | Recovery times out of line |
| 7 | Stale three-broker volume kept by `down -v` | Quorum crash | Regression run |
| 8 | Dead broker ports among measurement seeds | Random `FAILED_CHECK` | Failed cycles |
| 9 | F-4 and F-5 did not reach Debezium | CDC would have looked immune to flaky networks | Zero effect on P4 |
| 10 | `PG_IDLE_TX_TIMEOUT_S` never applied | Changing it would have done nothing | Audit of catalogue settings against code |
| 11 | H-1's burst settings not passed, and burst timed from load start | The crash would have missed the burst | Same audit |

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

**Update: this depends on the outage type (see F11).** It holds for a proxy cut and a broker stop, not for connection resets or a blackhole with idempotence off.

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

## F11. Whether the delivery timeout fires depends on how the outage looks to the client (Trial)

P2, 100 orders/s, 8 s outage, one run per row:

| Outage | Idempotence | First failure after fault starts | Failed during outage | Failed in total | Lost | Out of order | Slowest delivery |
|---|---|---|---|---|---|---|---|
| Proxy cut | on | 8.9 s (after restore) | 0 | 1,580 | 0 | 271 | 9.1 s |
| Proxy cut | off | 8.7 s (after restore) | 0 | 1,555 | 0 | 203 | 8.9 s |
| Blackhole, both directions | on | none | 0 | 0 | 0 | 0 | 8.1 s |
| Blackhole, both directions | off | 6.3 s | 1,127 | 4,934 | 0 | 386 | 14.0 s |
| Connection reset | on | 5.4 s | 642 | 2,206 | 0 | 283 | 13.4 s |
| Broker stop | on | 10.5 s (after restore) | 0 | 2,759 | 0 | 331 | 15.8 s |
| Broker stop | off | 10.7 s (after restore) | 0 | 4,702 | 0 | 500 | 16.2 s |
| Proxy cut, `RecordRetries=3` | on | 8.7 s (after restore) | 0 | 1,028 | 0 | 156 | 8.9 s |
| Proxy cut, `RecordRetries=3` | off | 8.5 s (after restore) | 0 | 1,535 | 0 | 158 | 8.7 s |

Evidence: `experiments/trials/2026-11-07/outage-types/`, `analysis/diagnostics/outage_types.sh`, `outage_summary.py`.
Limits: one run per row, 8 s outages, P2 only.

## F12. P2's retries prevent loss in a short outage, but reorder events (Trial)

In all 9 outages above, P2 lost nothing: every failed record was retried and delivered, with 0 `RETRY_EXHAUSTED`. P1 lost about 1,500 in the same kind of outage (F1). The cost is ordering: 156–500 out-of-order events in 8 of the 9 runs, the exception being the blackhole with idempotence on, which failed nothing. A retried older version lands after a newer one.

Retries therefore do help with an outage when the process survives. They do not help when it crashes: A-1 lost the hooked event every time.

## F13. Crash comparison across patterns (Trial, 2–3 repeats per case)

Same crash after commit: P1 and P2 lost the hooked event every time. P3, P4 and P5 lost nothing (A-1b, 3 cycles each). Window p50: 5 ms for P1 and P2, 240 ms for P3, about 530 ms for P4 and P5. A-5: 0 damage. A-4: no loss, up to 92 duplicates per crash, and 18–19 stale redeliveries in two cycles.

Evidence: `experiments/trials/2026-11-07/m6-crash-comparison/`.

## F14. The CDC window follows Debezium's poll interval (Trial)

| `poll.interval.ms` | Pattern | Window p50 | Window p99 | Window max |
|---|---|---|---|---|
| 100 | P4 | 105 ms | 196 ms | 245 ms |
| 100 | P5 | 103 ms | 193 ms | 205 ms |
| 500 | P4 | 509 ms | 919 ms | 996 ms |
| 500 | P5 | 540 ms | 943 ms | 1,006 ms |

So the roughly 530 ms window is a default setting, not a property of CDC. p50 is about 1× the interval and the maximum about 2×, which suggests two polling stages in series. That mechanism is an inference, not confirmed from Debezium's source. The P3-versus-P4 window difference must be reported with both poll intervals stated. No damage at either setting.

Evidence: `experiments/trials/2026-11-07/m6-poll-and-batch/`. One run per row.

## F15. Effective relay batch follows arrivals, not the configured limit (Trial)

P3, about 178 changes/s, `RELAY_BATCH_SIZE=100` unless stated:

| Poll interval | Batch mean | Batch max | Backlog max | Window p50 |
|---|---|---|---|---|
| 100 ms | 5.7 | 25 | 24 | 44 ms |
| 500 ms | 21 | 100 | 92 | 243 ms |
| 1,000 ms | 28 | 100 | 191 | 485 ms |
| 5,000 ms | 79 | 100 | 894 | 2,515 ms |
| 500 ms, `RELAY_BATCH_SIZE=1` | 1 | 1 | 1,342 | 4,809 ms |

Supports the hypothesis that effective batch = min(limit, rows arrived since the last poll). With batch size 1 the relay falls behind: backlog reached 1,342 rows.

Evidence: as F14. One run per row, no crash.

## F16. Shortened no-fault control passes for all five patterns (Trial)

10 minutes per pattern at 100 orders/s, without consumers: about 549,000 events in total, with 0 lost, ghost, duplicate or out-of-order events. This is not C0-1 itself, which needs one hour and consumers.

Evidence: `experiments/trials/2026-11-07/m6-control-10min/`.

## F17. Pre-cycle latency blips occur with the Mac idle (Trial, open)

P3's check in F16 read 275 ms while the full 10-minute p99 was 5.3 ms, with the Mac nearly idle. Earlier, 8 of 31 cycles were flagged at low host load. So these blips are not explained by use of the Mac. The cause is unknown.

## F18. Harness artifact: Toxiproxy's accept loop inflated database downtime (Trial)

Toxiproxy dials upstream inside its accept loop, so one dial to a dead address blocks every new client for about 25 s. That distorted failover twice: after the kill, and at the start of the next cycle, where many cycles had no working database for their first 20 s yet still reported OK. Fixed with a separate `toxiproxy-pg` instance, restarted at failover, plus `INVALID_NO_LOAD` and `load_failed`.

| | Before | After |
|---|---|---|
| Failed requests per cycle | thousands | 64–97 |
| P3 recovery after failover | 39–131 s | 0.2–0.5 s |

So P3's slow recovery was never real. Two causes stacked: this artifact and our 60 s relay timeout.

Evidence: `experiments/trials/2026-11-07/m7-failover-stuck-dial-artifact/` against `m7-failover-measurements/`.

## F19. CDC failover loss has two parts: the slot gap and Debezium's retry wait (Trial)

P5, 3 cycles each. D-1 and D-2's no-snapshot cell are the same configuration, and on the clean system they look alike, so the earlier 0 versus 58 was the artifact plus chance.

| Cell | Lost | Duplicates |
|---|---|---|
| D-1 | 53, 59, 1,861 | 0 |
| D-2, no snapshot | 71, 141, 1,906 | 0 |
| D-2, initial snapshot | 10, 25, 816 | 2,940–3,005 |

- **Slot gap (every cycle):** events committed on the new primary 0.5–1.4 s after failover, after the application reconnected and before the new slot existed.
- **Retry wait (3 of 9 cycles):** the old task entered Debezium's retry wait, delaying connector deletion, so the new connector appeared 10.5 s after failover instead of about 0.85 s. Lost events then span up to 10.6 s. The log line "Going to restart connector after N sec" appears exactly 3 times, once per affected cycle. Those cycles also lost 12–84 events committed up to 0.48 s before failover; why only those cycles is not yet established.
- **Snapshot:** all 851 lost events across the three snapshot cycles were intermediate versions (seq 1 with a later version existing). A snapshot restores each order's latest state, not its history.

Evidence: `experiments/trials/2026-11-07/m7-failover-measurements/`, `analysis/diagnostics/failover_summary.py`.

## F20. Ghost events appear only with replication lag (Trial)

D-1 on P1: 0 ghosts with no added lag (lag at failover 0 bytes). At 200 ms: 23 and 16 ghosts, with 35–37 KB and about 400–530 ms of lag at failover. Two cycles at 200 ms, one at 0.

## F21. Exactly-once removes connector-crash duplicates (Trial, single cycles)

E-3 with the default 60 s flush: P4 without exactly-once gave 3,548 duplicates and 1,563 stale redeliveries; P4 and P5 with it gave 0 and 0. Throughput was unchanged at this load (178–180 ops/s). This answers spec Section 2's open question: exactly-once source mode works with Debezium 3.7 on Postgres here.

Evidence: `experiments/trials/2026-11-07/m7-fault-trial/failover-and-eos-attempt1.csv`.

## F22. Graceful and crash restarts of Connect differ sharply (Trial, single cycles)

E-1 (restart): 0 duplicates. E-2 (crash), 1 s flush: 286 duplicates, against about 90 expected from rate × flush / 2. An earlier 5,425 came from our bug: the Debezium image overwrites `CONNECT_OFFSET_FLUSH_INTERVAL_MS`, so the worker stayed at 60 s. Now set through `OFFSET_FLUSH_INTERVAL_MS`.

## F23. Faults reproduced on demand (Trial, single cycles)

B-1: 1 out-of-order event. B-2: 89 duplicates, 35 out of order, 42 stale. F-3b: 96 duplicates despite the idempotent producer. A-3: 1,568 lost; A-3r: 1,261 lost; C-3r on P2: 3,182 lost; C-3 on P3: 0. C-1 on P1: 2,771 lost, equal to `send_failed`.
C-4 on P1 lost 525–782 events with `acks=all`, all with `send_failed` or undelivered records, which is not what its hypothesis tests. See the pending `lost_after_ack` decision.

## F24. With idempotence on, the same outage can lose everything in flight or nothing (Trial)

P1, identical 8 s proxy cut at 100 orders/s, idempotence on: one run lost 1,551 events, the next lost 0 with `send_failed` 0 and a slowest delivery of 9.1 s. Earlier runs lost 1,072 to 1,554. With idempotence off, the cut lost about 1,500 in each of the three runs so far.

So P1's loss under a cut is not a fixed function of outage length. The likely cause is whether requests were in flight when the connection died, but this is not confirmed. C-1 and C-2 need enough repeats to show the spread, and means alone would mislead. Reporting the distribution matters here.

Evidence: `experiments/trials/2026-11-07/smoke-checker-intermittent/`, F1.

## F25. Consumer strategies decide whether upstream duplicates become business damage (Trial)

Business damage (payments against paid orders), naive / dedup / upsert:

| Cycle | Upstream duplicates | Naive | Dedup | Upsert |
|---|---|---|---|---|
| No faults, P1–P5, 60 s | 0 | 0 | 0 | 0 |
| G-1 on P3 (3 cycles) | 0 | 1 | 0 | 0 |
| G-2 on P3 (2 cycles) | 0 | 1 | 1 | 1 |
| G-3 on P3 | 4 | 3 | 0 | 0 |
| G-3 on P4 | 3,697 | 1,206 | 0 | 0 |
| G-3 on P5 | 5,632 | 1,896 | 0 | 0 |
| G-4 on P3 | 0 | 0 | 0 | 0 |

G-1 and G-2 match their hypotheses, including that committing the offset first defeats dedup. G-3's P4 and P5 cycles ran at the default 60 s flush, so a Connect crash replays heavily.

G-4 showed no naive duplicates in its single cycle, against its hypothesis. Consumers commit after every record, so at most one uncommitted event exists when a member pauses, which may make the effect rare. Needs the full run.

Evidence: `experiments/trials/2026-11-07/m8-consumers/`.

## F26. The Mac sustains 1,000 orders/s on every pattern; window and cost depend on load (Trial)

60 s per pattern at 1,000 orders/s (about 1,790 changes/s), one run each, no faults: 0 failed requests and 0 damage throughout.

| Pattern | Latency p99 | Window p50 | Window p99 | DB CPU | DB CPU per 1,000 changes |
|---|---|---|---|---|---|
| P1 | 66 ms | 3 ms | 12 ms | 39% | 22% |
| P2 | 432 ms | 3 ms | 7 ms | 34% | 19% |
| P3 | 77 ms | 9 ms | 32 ms | 56% | 31% |
| P4 | 128 ms | 340 ms | 867 ms | 71% | 40% |
| P5 | 64 ms | 309 ms | 846 ms | 62% | 34% |

A-7's 1,000/s cells stay in. Cost order: dual write, then outbox with a relay, then CDC, with P4 heaviest. At 100 orders/s, P3's window was about 240 ms and P4 and P5's about 530 ms, so all three change with load. The analysis rule in spec Section 15 reports the window per load level for that reason.

Evidence: `experiments/trials/2026-11-07/capacity-1000/`.

## F27. Repeatability: loss and central metrics are stable, tail latency is not (Trial)

A-1 on P1, 30 cycles with the same seed: events ±0.1%, window p50 ±1%, throughput ±1%, hooked loss exactly 1 every time. Tail latency varied widely: p99 3–270 ms, window p99 7–120 ms. Ruled out: drift (correlation with cycle order 0.14), host load, Kafka (window p99 unrelated) and Postgres checkpoints (the slow cycles did not overlap them). Slow statements over 50 ms were mostly COMMIT (8 of 14, maximum 123 ms), which points to disk sync inside Docker's VM on macOS. Durability settings were not changed. Reported as a test-bed limitation: p99 is given with its spread, and medians are the primary numbers.

Evidence: `experiments/trials/2026-11-07/repeatability/`, `f24-and-cdc-diagnostics/slow.txt`, `analysis/diagnostics/tail_checkpoints.py`.

## F28. F24's loss is decided per partition, in three modes (Trial)

C-2 on P1, 20 cycles with the same seed: totals from 0 to 2,408. Broken down by partition (Kafka's murmur2), each partition took one of three paths during the cut:
- **held:** nothing lost; records wait and are delivered when the connection returns;
- **timed out one by one:** about 800 lost, failing continuously as each record reaches the 5 s delivery timeout;
- **failed at once with a dial error:** about 500 lost, all at once.

Our hypothesis that a request in flight at the cut spares a partition was tested with per-partition traces and ruled out: no partition had one. Which mode a partition takes depends on franz-go's per-partition retry state, not yet pinned down. This is client behaviour, not a harness bug. Report the distribution with the cause marked open.

Evidence: `experiments/trials/2026-11-07/repeatability/`, `f24-and-cdc-diagnostics/`, `analysis/diagnostics/f24_partitions.py`, `f24_inflight.py`, `f24_timing.py`.

## F29. CDC does not lose events on a retry restart when the slot survives (Trial)

X-1 (diagnostic catalogue): Debezium's Postgres connection cut for 5 s, 10 cycles each on P4 and P5. The retry path fired in 20 of 20 cycles. **Lost: 0 in all 20.** Duplicates about 3,500 and stale redeliveries about 1,560 per cycle, because the slot's `confirmed_flush_lsn` did not move within a cycle: Debezium confirms it only on an offset flush (60 s), so a restart replays from the slot's start.

In failover the slot dies with the primary (PG16). Events Debezium had read but not sent survive a clean stop (79–151 delivered late in normal cycles) but are dropped on a retry restart, and cannot be re-read. The raw topic, read with Kafka's own consumer from offset 0 on every partition, contains none of the missing events, and committed minus lost equals the raw record count exactly in all three cycles checked.

So the paper should say: CDC loses events at PG16 failover because the slot is lost, and the amount depends on whether Debezium's task stops cleanly. Not: "CDC loses events". Our configuration affects duplicates (flush interval) but not loss. Versions: Debezium 3.7.0.Final, Connect 4.3.1, Postgres 16.15. No matching known issue found (see `docs/RELATED_WORK.md`).

Evidence: `experiments/trials/2026-11-07/cdc-retry-diagnostic/`, `f24-and-cdc-diagnostics/`, `analysis/diagnostics/cdc_retry.py`, `raw_topic_check.py`, `experiments/diagnostics.yaml`.

## F30. The client can report failure for records the broker stored (Trial)

P1, idempotence off, 8 s proxy cut: 1,594 `SEND_FAILED` markers against 1,592 lost events. Every lost event had a marker; 2 records reported as failed were in Kafka. With idempotence off, a request whose response is lost can be failed by the client after the broker wrote it. This is the mechanism behind F-2's duplicates when P2 retries such a record. Found by the acceptance gate, whose test had wrongly assumed failures and losses match one for one.

Evidence: `experiments/trials/2026-11-07/smoke-checker-f30/`.

## Hypotheses to keep and test

Record these as written, then mark them supported or refuted. Do not reword them after the fact.

- **H-REORDER: retries swap loss for reordering.** Early support in F12. Test: compare `lost`, `out_of_order` and `stale_redelivery` for P2 against P1 (no retries) and P3–P5 (ordered publishing) in C-1, C-2, C-3r and A-3r. These columns are written for every cycle; C-2 and A-3r cover only the patterns the catalogue lists.
- **H-BATCH: effective batch = min(`RELAY_BATCH_SIZE`, rows arrived since the last poll).** Early support in F15. Original A-7 hypothesis, kept for the record: "Duplicate probability and size grow with batch size."

- **A-3:** "Every event waiting in the in-memory retry queue is lost; retries do not fix dual write." At risk from F1: the retry path may not run during an outage.
- **C-3:** "P2 loses its queued retries; the P3 outbox survives the publisher crash." Same risk.
- **B-3:** Idempotence on: in-flight records cannot time out. Off: records time out and P2 duplicates them.

## Analysis rule (fixed before the final runs)

Every result is reported over all cycles and again excluding `pre_flag` cycles, with "all cycles" as the primary number (spec Section 15).

## Design decisions the paper should explain

- `out_of_order` counts first deliveries only; `stale_redelivery` counts repeats below a newer version (spec 11.2).
- Topics use `LogAppendTime`, so the window uses the broker's clock (spec 5.3).
- Crash points are armed after warm-up at a random point (spec 7).
- C-4 runs both cells with idempotence off, so only `acks` differs.
