# Trial data, 2026-11-07

**Log storm.** From 01:05:57 UTC until about 02:27 UTC, Kafka Connect wrote about 20 MB/s of warnings to its log, after the broker was recreated and Connect's internal topics were lost. Everything in this folder except `clean-rerun/` and `m5-acceptance/` ran during that storm, so its latency and timing numbers are unreliable.

- `clean-rerun/`: latency and outage tests repeated after the storm was fixed.
- `m5-acceptance/`: Milestone 5 acceptance on the clean system.
- `m5-acceptance-during-log-storm/`: the first attempt, kept for the record. Includes the 4 `FAILED_RESET` cycles.
