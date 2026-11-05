# Test Cases

Environment: Windows, Python 3.14 venv, PySpark 4.3.0, PostgreSQL 18, Java 21.
Before every test: delete `checkpoints\` and `data\incoming\*`, then `TRUNCATE events;`.

## Test 1: CSV generation is correct

Command: `python data_generator.py --files 3 --events-per-file 20 --bad-rate 0`

| Check | Expected | Actual |
|---|---|---|
| Number of CSV files in `data\incoming` | 3 | |
| Data rows per file (excluding header) | 20 | |
| Columns per file | 9 (event_id, user_id, event_type, product_id, product_name, category, price, quantity, event_time) | |
| No temp files left behind (`.name.tmp`) | none | |
| `event_type` values | only `view` or `purchase` | |

Verify: `(Import-Csv data\incoming\<file>).Count` and `dir data\incoming -Force`.

## Test 2: Spark detects new files

Steps: start the stream with an empty `data\incoming`, wait for startup, then run `python data_generator.py --files 3`.

| Check | Expected | Actual |
|---|---|---|
| A `[batch N]` line appears for new files | within about 2 trigger intervals (10 s) of each file | |
| Total `received` across batches | 60 | |
| Files already processed are not re-read after restart | no new batch lines on restart without new files | |

## Test 3: Transformations are correct (hand-made file)

Create the file with the commands below, then move it into `data\incoming`. The file has 8 rows: 3 valid
(one needs trimming and lowercasing), 1 exact duplicate of a valid row, and 4 defective rows.

| Check | Expected | Actual |
|---|---|---|
| received / written / rejected | 8 / 3 / 4 | |
| Rejection reasons | missing_user_id 1, invalid_price 1, negative_price 1, invalid_event_type 1 | |
| Row `t7` stored with `event_type = 'view'` | yes (trimmed and lowercased) | |
| Duplicate `t1` stored once | yes | |

## Test 4: Data is written to PostgreSQL without errors

Use the same run as Test 3 and a larger generator run (`--files 5`).

| Check | Expected | Actual |
|---|---|---|
| Stream log shows no exceptions | none | |
| `SELECT count(*) FROM events` | equals the sum of `written` in the log | |
| Duplicate `event_id`s | 0 (`SELECT event_id, count(*) FROM events GROUP BY 1 HAVING count(*) > 1`) | |
| No rows with NULL `user_id` or negative `price` | 0 | |
| `ingested_at` is populated | no NULLs | |

## Test 5: Performance (latency and throughput)

See `performance_metrics.md`.