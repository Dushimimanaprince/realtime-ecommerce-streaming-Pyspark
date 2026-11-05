# Performance Metrics

Environment: Windows laptop, Spark `local[*]`, 4 shuffle partitions, 5 s trigger, PostgreSQL 18 on localhost,
JDBC append via `foreachBatch`. Raw logs are in `docs/evidence/`.

## Method
- **Latency** = `ingested_at - event_time` (PostgreSQL insert time minus the generator's event timestamp).
- **Throughput** = `written / took` per micro-batch, from the stream log.
- Each scenario ran from a clean state: checkpoint deleted, `data/incoming` emptied, `events` truncated.
- Batch 0 includes one-time startup cost (JVM, JDBC driver, first connection) and is reported separately.

## Results

| | Scenario A: light | Scenario B: heavy |
|---|---|---|
| Load | 20 events / 5 s (10 files) | 500 events / 1 s (20 files) |
| Events received / written / rejected | 200 / 185 / 15 | 10,000 / 9,504 / 496 |
| Micro-batches | 9 | 4 |
| Latency avg | 6.93 s | 8.18 s |
| Latency p50 | 6.45 s | 7.03 s |
| Latency p95 | 11.67 s | 14.66 s |
| Latency max | 11.67 s | 14.66 s |
| Batch 0 time (warm-up) | 4.63 s | 8.73 s |
| Batch time, batches 1+ | 0.58 to 1.35 s | 0.98 to 1.41 s |
| Throughput, batches 1+ | 13 to 48 rows/s | 856 to 3,710 rows/s |

## Observations
1. **The stream keeps up.** After batch 0, every batch finished well inside the 5 s trigger, even when
   scenario B delivered 5,500 rows in one batch (1.41 s, about 3,700 rows/s).
2. **Throughput depends on batch size.** Small batches (20 rows) take roughly 0.6 s, so they show only
   about 30 rows/s. Large batches amortize that fixed per-batch cost (job scheduling and the JDBC
   connection), reaching thousands of rows/s.
3. **Warm-up dominates the latency tail.** Batch 0 exceeded the trigger interval in both scenarios
   (4.63 s and 8.73 s). In B, events that arrived during the 8.7 s first batch waited behind it,
   which explains p95 and max both being 14.66 s. After warm-up the stream drained the backlog within one batch.
4. **Median latency is above the trigger interval.** It reflects the wait for the next 5 s trigger,
   file discovery, and processing time. A shorter trigger (1 to 2 s) would lower it at the cost of more, smaller batches.
5. **Rejection rate** was 7.5% in A (15 of 200) and 4.96% in B (496 of 10,000), consistent with the 5% bad-rate setting.

## Limitations
- Single machine: the generator, Spark and PostgreSQL compete for the same CPU and disk.
- One run per scenario, so the figures show behavior, not statistical confidence.
- JDBC appends are not tuned (no batch-size or rewrite options).
- Latency uses the generator's local clock and PostgreSQL's `LOCALTIMESTAMP`, which are the same machine and time zone.