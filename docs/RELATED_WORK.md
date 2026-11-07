# Related work notes

First pass, 2026-11-07, general web search only. IEEE Xplore, ACM DL and Google Scholar have not been searched, so this does not establish novelty.

No peer-reviewed quantitative comparison of dual write, outbox and CDC under fault injection turned up. Absence from a web search is weak evidence.

To read in full:
- "Machine-Checked Dual-Write Recovery from a Commit Log", arXiv 2608.00501: https://arxiv.org/pdf/2608.00501 . Closest academic work found. Only excerpts seen; methodology unknown.
- Jepsen, Bufstream 0.1.0: https://jepsen.io/analyses/bufstream-0.1.0 . Reports Kafka transaction protocol issues (KAFKA-17754). Relevant to E-3. Check whether fixed in Kafka 4.3.2.
- Java Code Geeks, outbox and CDC: https://www.javacodegeeks.com/2026/10/the-outbox-pattern-and-change-data-capture-solving-the-dual-write-problem-with-debezium.html . Informal simulation, 100,000 orders, 1% crash chance.
- Ajay-04/kafka-outbox: https://github.com/Ajay-04/kafka-outbox . Crash-recovery harness, code demo.
