# Related work notes

First pass, 2026-11-07, general web search only. IEEE Xplore, ACM DL and Google Scholar have not been searched, so this does not establish novelty.

No peer-reviewed quantitative comparison of dual write, outbox and CDC under fault injection turned up. Absence from a web search is weak evidence.

To read in full:
- "Machine-Checked Dual-Write Recovery from a Commit Log", arXiv 2608.00501: https://arxiv.org/pdf/2608.00501 . Closest academic work found. Only excerpts seen; methodology unknown.
- Jepsen, Bufstream 0.1.0: https://jepsen.io/analyses/bufstream-0.1.0 . Reports Kafka transaction protocol issues (KAFKA-17754). Relevant to E-3. Check whether fixed in Kafka 4.3.2.
- Java Code Geeks, outbox and CDC: https://www.javacodegeeks.com/2026/10/the-outbox-pattern-and-change-data-capture-solving-the-dual-write-problem-with-debezium.html . Informal simulation, 100,000 orders, 1% crash chance.
- Ajay-04/kafka-outbox: https://github.com/Ajay-04/kafka-outbox . Crash-recovery harness, code demo.

## Debezium loss investigation (2026-11-07)

Versions under test: Debezium Postgres connector 3.7.0.Final, Kafka Connect and clients 4.3.1, Postgres 16.15. No 3.x issue matching "queued events dropped on a retriable restart" was found in a web search; the Debezium JIRA has not been searched directly.
- Debezium docs: a fault should not lose events, only repeat some. So a confirmed drop outside a slot loss would be a bug. https://debezium.io/documentation/reference/stable/connectors/postgresql.html
- DBZ-6204, "Data loss upon connector restart", fixed in 2.2; mechanism not read. https://debezium.io/releases/2.2/release-notes
- Zalando (2025): offset can fall behind the slot after a restart; a `trust_slot` option skips events between offset and slot, the closest documented route to loss. https://engineering.zalando.com/posts/2025/12/contributing-to-debezium.html
- Failover guidance for PG < 17, where slots are not copied: https://aiven.io/docs/products/kafka/kafka-connect/howto/debezium-source-connector-pg-node-replacement , https://franejelavic.github.io/writing/debezium-postgresql-production-lessons/
