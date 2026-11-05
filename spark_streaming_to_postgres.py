"""Spark Structured Streaming job: reads event CSV files, cleans them and writes to PostgreSQL."""
import os
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

PROJECT_ROOT = Path(__file__).resolve().parent
INPUT_DIR = PROJECT_ROOT / "data" / "incoming"
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints" / "events"

TRIGGER_INTERVAL = "5 seconds"
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
VALID_EVENT_TYPES = ["view", "purchase"]

JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"
JDBC_URL = "jdbc:postgresql://localhost:5432/ecommerce_streaming"
JDBC_PROPERTIES = {
    "user": os.getenv("PG_USER", "streaming_user"),
    "password": os.getenv("PG_PASSWORD", "streaming_pass"),  # dev only
    "driver": "org.postgresql.Driver",
}
TARGET_TABLE = "events"



# Every column is read as text. Types are converted (and bad values handled) in the cleaning step.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("event_time", StringType(), True),
])


def create_spark_session() -> SparkSession:
    """Create a local Spark session for the streaming job."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName("Ecommerce Streaming to PostgreSQL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", JDBC_PACKAGE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_events(spark: SparkSession) -> DataFrame:
    """Watch the input folder and read each new CSV file as part of a stream."""
    return (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(EVENT_SCHEMA)
        .load(INPUT_DIR.as_uri())
    )


def _clean_text(column: str) -> Column:
    """Trim a text column and turn empty strings into null."""
    trimmed = F.trim(F.col(column))
    return F.when(trimmed == "", None).otherwise(trimmed)


def clean_events(events: DataFrame) -> DataFrame:
    """Convert types and add a `rejection_reason` column (null when the event is valid)."""
    df = events
    for column in EVENT_SCHEMA.fieldNames():
        df = df.withColumn(column, _clean_text(column))

    df = (
        df.withColumn("event_type", F.lower(F.col("event_type")))
        .withColumn("price", F.col("price").try_cast("decimal(10,2)"))
        .withColumn("quantity", F.col("quantity").try_cast("int"))
        .withColumn("event_time", F.try_to_timestamp(F.col("event_time"), F.lit(TIMESTAMP_FORMAT)))
    )

    rejection_reason = (
        F.when(F.col("event_id").isNull(), "missing_event_id")
        .when(F.col("user_id").isNull(), "missing_user_id")
        .when(~F.col("event_type").isin(VALID_EVENT_TYPES) | F.col("event_type").isNull(),
              "invalid_event_type")
        .when(F.col("price").isNull(), "invalid_price")
        .when(F.col("price") < 0, "negative_price")
        .when(F.col("quantity").isNull() | (F.col("quantity") < 0), "invalid_quantity")
        .when(F.col("event_time").isNull(), "invalid_event_time")
        .when(
            F.col("product_id").isNull() | F.col("product_name").isNull() | F.col("category").isNull(),
            "missing_product_info",
        )
    )
    return df.withColumn("rejection_reason", rejection_reason)

def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Called by Spark once per micro-batch with a normal (non-streaming) DataFrame."""
    batch_df.cache()  # we use it 3 times below; avoid recomputing
    total = batch_df.count()
    if total == 0:
        batch_df.unpersist()
        return

    rejects = (
        batch_df.filter(F.col("rejection_reason").isNotNull())
        .groupBy("rejection_reason").count().collect()
    )
    rejected = sum(r["count"] for r in rejects)

    valid = (
        batch_df.filter(F.col("rejection_reason").isNull())
        .drop("rejection_reason")
        .dropDuplicates(["event_id"])
    )
    written = valid.count()
    valid.write.jdbc(JDBC_URL, TARGET_TABLE, mode="append", properties=JDBC_PROPERTIES)

    reasons = {r["rejection_reason"]: r["count"] for r in rejects}
    print(f"[batch {batch_id}] received={total} written={written} rejected={rejected} {reasons}")
    batch_df.unpersist()

def main() -> None:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    events = clean_events(read_events(spark))

    query = (
        clean_events(read_events(spark))
        .writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_DIR.as_uri())
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping the stream...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()