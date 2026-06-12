# Ingestion Benchmark — Direct COPY vs. Kafka Streaming

Both paths land the same rows in the same PostgreSQL bronze tables.
`direct` = CSV -> server-side COPY. `kafka` = CSV -> producer -> Kafka -> consumers -> COPY. Times are end-to-end, including deduplication (`ON CONFLICT DO NOTHING`).

| When (UTC) | Mode | Input | Rows landed | Seconds | Rows/s |
|---|---|---|---:|---:|---:|
| 2026-06-12T17:21:45+00:00 | direct | bench_sample_500000.csv.sample | 499,383 | 8.84 | 56,492 |
| 2026-06-12T17:23:13+00:00 | kafka | bench_sample_500000.csv.sample | 499,383 | 17.89 | 27,914 |
| 2026-06-12T17:24:09+00:00 | direct | bench_sample_2000000.csv.sample | 1,997,447 | 36.27 | 55,070 |
| 2026-06-12T17:25:50+00:00 | kafka | bench_sample_2000000.csv.sample | 1,997,447 | 65.45 | 30,517 |

**Best direct:** 56,492 rows/s · **Best kafka:** 30,517 rows/s · direct is **1.9×** faster end-to-end.

The trade-off: Kafka adds serialization, network hops, and consumer-side batching, but provides decoupling, replayability, back-pressure, and multiple independent consumers — none of which the direct path offers.
