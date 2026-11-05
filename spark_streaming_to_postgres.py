"""Spark Structured Streaming job: reads event CSV files, cleans them and writes to PostgreSQL."""
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

PROJECT_ROOT = Path(__file__).resolve().parent
INPUT_DIR = PROJECT_ROOT / "data" / "incoming"
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints" / "events"

TRIGGER_INTERVAL = "5 seconds"

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


def main() -> None:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    events = read_events(spark)

    query = (
        events.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
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