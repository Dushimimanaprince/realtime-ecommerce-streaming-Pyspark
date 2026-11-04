# Real-Time Data Ingestion with Spark Structured Streaming and PostgreSQL

A real-time pipeline that simulates e-commerce user activity: a Python script
generates CSV events, Spark Structured Streaming processes them as they arrive,
and the cleaned data is stored in PostgreSQL.

## Components
- `data_generator.py`: creates fake view/purchase events as CSV files
- `spark_streaming_to_postgres.py`: streaming job (read, clean, write)
- `postgres_setup.sql`: database and table creation

Setup and run instructions will be in `user_guide.md`.