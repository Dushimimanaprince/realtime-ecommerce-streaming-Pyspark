# Real-Time E-Commerce Event Ingestion with Spark Structured Streaming and PostgreSQL

A streaming pipeline that simulates an online store's activity (product views and purchases),
ingests it with PySpark Structured Streaming, validates and cleans each event, and stores the
valid ones in PostgreSQL. Built as an AmaliTech Data Engineering lab.

## How it works

```
data_generator.py -> data/incoming/*.csv -> Spark Structured Streaming -> PostgreSQL (events)
```

1. **Generator** writes a new CSV file every few seconds, with about 5% deliberately corrupted rows.
2. **Spark** reads new files in 5-second micro-batches using an explicit schema.
3. **Cleaning** trims and normalizes values, converts types safely, and assigns a `rejection_reason` to invalid rows.
4. **Sink** (`foreachBatch` + JDBC) de-duplicates by `event_id` and appends valid rows to PostgreSQL. Rejected rows are counted by reason in the log.

See `docs/system_architecture.png` for the diagram and `docs/project_overview.md` for the design decisions.

## Project structure

```
.
├── src/
│   ├── data_generator.py                 # simulates e-commerce events as CSV files
│   └── spark_streaming_to_postgres.py    # streaming job: read, clean, write
├── sql/postgres_setup.sql                # role, database and events table
├── config/postgres_connection_details.txt
├── scripts/reset.ps1                     # clears checkpoints, input files and the table
├── docs/
│   ├── project_overview.md
│   ├── user_guide.md
│   ├── test_cases.md
│   ├── performance_metrics.md
│   ├── system_architecture.png
│   └── evidence/                         # logs and screenshots from the test runs
├── data/incoming/                        # generated CSVs (git-ignored)
├── checkpoints/                          # Spark checkpoints (git-ignored)
└── requirements.txt
```

## Tech stack

Python 3.14, PySpark 4.3.0, Java JDK 21, PostgreSQL 18, PostgreSQL JDBC driver 42.7.4.

## Quick start

Full instructions, including Windows Hadoop setup and troubleshooting, are in
[`docs/user_guide.md`](docs/user_guide.md).

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# create the database and table
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -f sql\postgres_setup.sql

# terminal 1: start the stream
python src\spark_streaming_to_postgres.py

# terminal 2: generate data
python src\data_generator.py --files 10
```

Each micro-batch prints a line like:

```
[batch 1] received=40 written=37 rejected=3 {'invalid_event_type': 2, 'negative_price': 1}
```

Check the result:

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -d ecommerce_streaming -c "SELECT count(*) FROM events;"
```

Reset between runs with `.\scripts\reset.ps1`.

## Data validation

Rows are rejected, not silently dropped, for one of these reasons: `missing_event_id`,
`missing_user_id`, `invalid_event_type`, `invalid_price`, `negative_price`, `invalid_quantity`,
`invalid_event_time`, `missing_product_info`.

## Testing and performance

- Test cases and results: [`docs/test_cases.md`](docs/test_cases.md)
- Latency and throughput measurements: [`docs/performance_metrics.md`](docs/performance_metrics.md)

## Design notes

- Files are written under a hidden temp name and renamed, so Spark never reads a partial file.
- Duplicates are removed per micro-batch; the `event_id` primary key is the final safeguard.
- Latency is measured as `ingested_at - event_time`, where PostgreSQL stamps `ingested_at` on insert.
- The database password in this repo is a development default; override it with `PG_PASSWORD`.

## Limitations

Single-machine run, file-based source instead of Kafka, rejected rows are logged but not stored.