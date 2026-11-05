# Project Overview: Real-Time E-Commerce Event Ingestion

## Purpose
A streaming pipeline that simulates an online store's activity (product views and purchases),
ingests it with Spark Structured Streaming, validates and cleans it, and stores the valid
events in PostgreSQL for analysis.

## Architecture
data_generator.py  ->  data/incoming/*.csv  ->  Spark Structured Streaming  ->  PostgreSQL (events)

1. **Generator** (`data_generator.py`): writes a new CSV file every few seconds. About 5% of rows
   are deliberately corrupted (missing user, bad price, bad event type) to exercise validation.
   Each file is written under a hidden temp name and then renamed, so Spark never reads half a file.
2. **Stream** (`spark_streaming_to_postgres.py`): reads new files as they appear, in micro-batches
   every 5 seconds, using an explicit all-string schema.
3. **Cleaning**: trims text, lowercases `event_type`, converts price/quantity/time with
   `try_cast` and `try_to_timestamp` (ANSI-safe: bad values become NULL, not errors), and sets a
   `rejection_reason` for each invalid row.
4. **Sink** (`foreachBatch` + JDBC): per micro-batch, valid rows are de-duplicated on `event_id`
   and appended to PostgreSQL; rejected rows are counted by reason in the log.
5. **Database** (`postgres_setup.sql`): `events` table with a primary key on `event_id`,
   CHECK constraints, and an `ingested_at` default used to measure latency.

## Data model
| Column | Type | Notes |
|---|---|---|
| event_id | VARCHAR(36) PK | unique per event |
| user_id | VARCHAR(10) NOT NULL | |
| event_type | VARCHAR(20) | `view` or `purchase` |
| product_id, product_name, category | text | |
| price | NUMERIC(10,2) | >= 0 |
| quantity | INTEGER | >= 0, 0 for views |
| event_time | TIMESTAMP NOT NULL | when the event happened |
| ingested_at | TIMESTAMP | set by PostgreSQL on insert |

## Key design decisions
- **Rejection reasons instead of silent drops:** every bad row is counted and explained.
- **Checkpointing:** `checkpoints/events` lets the stream resume without reprocessing files.
- **Per-batch de-duplication:** stream-wide dedup needs unbounded state; the primary key is the final safety net.
- **`event_id` as VARCHAR:** avoids a JDBC string-to-UUID type mismatch.
- **Local time everywhere:** the generator and Spark both use local time, so latency is not skewed by time zones.

## Technology
Python 3.14, PySpark 4.3.0, Java 21, PostgreSQL 18 (JDBC driver 42.7.4), Windows with Hadoop native binaries (see `user_guide.md`).

## Limitations
Single-machine run, file-based source (not Kafka), JDBC inserts without batching tuning,
rejected rows are logged but not stored.    