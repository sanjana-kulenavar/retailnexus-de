# ingestion/kafka_consumer/pos_to_bronze.py

# ─────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────

import os
import sys

# Add the processing/spark_jobs directory to Python's path
# so we can import our get_spark_session utility
project_root = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)
)))
sys.path.insert(0, os.path.join(project_root, "processing", "spark_jobs"))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, BooleanType
)
from utils.spark_session import get_spark_session
from dotenv import load_dotenv

load_dotenv()


# ─────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_POS", "pos-transactions")

S3_BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE", "retailnexus-bronze-dev")

# The S3 path where Bronze files are written
# s3a:// is the correct protocol for PySpark (not s3://)
BRONZE_PATH = f"s3a://{S3_BUCKET_BRONZE}/sales/"

# How often PySpark collects messages and writes a batch to S3
# 30 seconds = collect 30 seconds worth of Kafka messages → write to S3
TRIGGER_SECONDS = 30

# Checkpoint location: Spark writes its progress here
# This tells Spark which Kafka messages it has already processed
# If the consumer restarts, it continues from where it left off
# NEVER delete this folder while the consumer is running
CHECKPOINT_PATH = "/tmp/retailnexus_checkpoints/pos_bronze/"


# ─────────────────────────────────────────────────────────────────────
# SCHEMA DEFINITION
# ─────────────────────────────────────────────────────────────────────

# This defines the structure of JSON messages coming from Kafka
# It must match the fields in your POSTransaction Pydantic schema exactly
# Think of this like defining columns in an Oracle CREATE TABLE statement

POS_SCHEMA = StructType([
    # nullable=False means this field CANNOT be null (required)
    # nullable=True means this field CAN be null (optional)
    StructField("transaction_id", StringType(),  nullable=False),
    StructField("store_id",       StringType(),  nullable=False),
    StructField("terminal_id",    StringType(),  nullable=True),
    StructField("cashier_id",     StringType(),  nullable=True),
    StructField("product_id",     StringType(),  nullable=False),
    StructField("quantity",       IntegerType(), nullable=False),
    StructField("unit_price",     FloatType(),   nullable=False),
    StructField("total_amount",   FloatType(),   nullable=False),
    StructField("payment_method", StringType(),  nullable=True),
    StructField("transaction_ts", StringType(),  nullable=False),
    StructField("is_voided",      BooleanType(), nullable=False),
    StructField("audit_flag",     StringType(),  nullable=True),
])


# ─────────────────────────────────────────────────────────────────────
# MAIN CONSUMER FUNCTION
# ─────────────────────────────────────────────────────────────────────

def run_consumer():
    """
    Reads POS transactions from Kafka and writes them to S3 Bronze.

    HOW PYSPARK STRUCTURED STREAMING WORKS:
    ────────────────────────────────────────────────────────────────
    1. spark.readStream → connects to Kafka, subscribes to topic
       Creates a "streaming DataFrame" — data flows in continuously

    2. Every TRIGGER_SECONDS seconds → Spark reads new messages
       from Kafka (messages since the last batch)

    3. Parse the JSON → extract all fields into separate columns

    4. Add metadata columns → processing timestamp, pipeline name

    5. Filter bad rows → remove rows where JSON parsing failed

    6. Write to S3 → Parquet files, partitioned by year/month/day/store

    7. Update checkpoint → save position in Kafka (offset)

    8. Repeat from step 2 indefinitely
    ────────────────────────────────────────────────────────────────
    """

    print("=" * 60)
    print("  ReSA Kafka → S3 Bronze Consumer — RetailNexus DE")
    print("=" * 60)
    print(f"  Reading from : Kafka topic '{KAFKA_TOPIC}'")
    print(f"  Writing to   : {BRONZE_PATH}")
    print(f"  Batch trigger: every {TRIGGER_SECONDS} seconds")
    print(f"  Checkpoint   : {CHECKPOINT_PATH}")
    print("=" * 60)

    # Create Spark session with all required configurations
    spark = get_spark_session("resa-kafka-to-bronze")


    # ── STEP 1: CONNECT TO KAFKA ──────────────────────────────────────
    #
    # readStream vs read:
    #   spark.read → reads a fixed dataset once (batch processing)
    #   spark.readStream → reads a continuous stream (streaming)
    #
    # "startingOffsets": "latest" means:
    #   Start from NEW messages only (not old messages already in Kafka)
    #   If you restart the consumer, it uses the checkpoint to pick up
    #   from where it stopped (ignoring "latest")
    # ─────────────────────────────────────────────────────────────────

    raw_kafka_df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        # Maximum messages per trigger — limits memory usage per batch
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )

    # At this point, raw_kafka_df has these Kafka system columns:
    # ┌──────────────────────────────────────────────────────────┐
    # │ key       │ bytes  │ our store_id (message key)          │
    # │ value     │ bytes  │ our JSON transaction (message body) │
    # │ topic     │ string │ "pos-transactions"                  │
    # │ partition │ int    │ which Kafka partition (0, 1, or 2)  │
    # │ offset    │ long   │ position in the partition           │
    # │ timestamp │ time   │ when Kafka received the message     │
    # └──────────────────────────────────────────────────────────┘
    # The actual transaction data is in the 'value' column as bytes


    # ── STEP 2: PARSE JSON FROM KAFKA ────────────────────────────────
    #
    # The 'value' column contains bytes → we need text → then parse JSON
    #
    # Step A: Cast bytes to string
    #   F.col("value").cast("string") → converts bytes to JSON text
    #
    # Step B: Parse JSON text to structured data
    #   F.from_json(json_string, schema) → creates a struct column
    #   where each field in the schema becomes a nested column
    #
    # Step C: Select individual fields from the struct
    #   F.col("data.transaction_id") → extracts one field from struct
    # ─────────────────────────────────────────────────────────────────

    parsed_df = (
        raw_kafka_df
        # Cast the value bytes to a JSON string
        .withColumn("json_str", F.col("value").cast("string"))
        # Parse the JSON string into a struct using our schema
        # 'data' is now a column containing all our transaction fields
        .withColumn("data", F.from_json(F.col("json_str"), POS_SCHEMA))
        # Extract each field from the struct into its own column
        # and keep the Kafka timestamp for monitoring
        .select(
            F.col("data.transaction_id"),
            F.col("data.store_id"),
            F.col("data.terminal_id"),
            F.col("data.cashier_id"),
            F.col("data.product_id"),
            F.col("data.quantity"),
            F.col("data.unit_price"),
            F.col("data.total_amount"),
            F.col("data.payment_method"),
            F.col("data.transaction_ts"),
            F.col("data.is_voided"),
            F.col("data.audit_flag"),
            # Rename Kafka's timestamp column to avoid confusion
            F.col("timestamp").alias("kafka_received_at"),
        )
    )


    # ── STEP 3: ADD PARTITION COLUMNS AND METADATA ────────────────────
    #
    # WHY PARTITION BY DATE?
    #   Partitioning organises files into folders by date.
    #   When you later query "show me sales from Jan 15",
    #   Spark only reads the Jan 15 folder — skipping all other days.
    #   This makes queries 10-100x faster on large datasets.
    #
    # The S3 path becomes:
    #   sales/year=2025/month=06/day=15/store_id=NL-001/part-00000.parquet
    #
    # METADATA COLUMNS:
    #   These are audit columns — they tell you WHEN and HOW data was
    #   processed. Identical in purpose to Oracle ReSA audit columns
    #   like INSERT_DATE, LAST_MODIFIED, SOURCE_SYSTEM.
    # ─────────────────────────────────────────────────────────────────

    enriched_df = (
        parsed_df
        # Convert transaction_ts string → proper timestamp type
        # This enables date extraction functions below
        .withColumn(
            "transaction_ts",
            F.to_timestamp("transaction_ts")
        )
        # Extract year, month, day for partition folders
        .withColumn("year",  F.year("transaction_ts"))
        .withColumn("month", F.month("transaction_ts"))
        .withColumn("day",   F.dayofmonth("transaction_ts"))
        # When this pipeline processed the record
        .withColumn("ingested_at",   F.current_timestamp())
        # Pipeline name — useful when multiple pipelines write to same bucket
        .withColumn("pipeline_name", F.lit("resa-kafka-bronze"))
        # Source system — mirrors SOURCE_SYSTEM concept in Oracle ETL
        .withColumn("source_system", F.lit("POS_KAFKA_STREAM"))
        # Data layer marker — makes it clear this is Bronze (raw) data
        .withColumn("data_layer",    F.lit("BRONZE"))
        # ── DATA QUALITY: Remove rows where JSON parsing failed ────────
        # If from_json() cannot parse a message, it sets fields to null
        # We filter those out here to keep Bronze data clean
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("store_id").isNotNull())
        .filter(F.col("total_amount").isNotNull())
    )


    # ── STEP 4: WRITE TO S3 BRONZE ────────────────────────────────────
    #
    # writeStream vs write:
    #   df.write → writes data once (batch)
    #   df.writeStream → writes data continuously (streaming)
    #
    # outputMode("append"):
    #   Only add new records, never update or delete existing ones
    #   Bronze layer is immutable — we never change data once written
    #   This is a core principle: Bronze = raw, permanent record
    #
    # partitionBy("year", "month", "day", "store_id"):
    #   Creates this folder structure in S3:
    #   sales/year=2025/month=06/day=15/store_id=NL-001/part-0000.parquet
    #
    # trigger(processingTime="30 seconds"):
    #   Collect 30 seconds of Kafka messages → write one batch
    #   Balances latency vs file count (too frequent = too many small files)
    # ─────────────────────────────────────────────────────────────────

    streaming_query = (
        enriched_df
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("year", "month", "day", "store_id")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    print(f"\n✅ Streaming consumer started successfully")
    print(f"   First batch writes to S3 in {TRIGGER_SECONDS} seconds...")
    print(f"   Watching Kafka for incoming transactions...")
    print(f"   Press Ctrl+C to stop\n")

    # awaitTermination() keeps this script running until Ctrl+C
    # Without it, the script would exit immediately after .start()
    try:
        streaming_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n  ⏹  Consumer stopped by user")
        streaming_query.stop()
        spark.stop()


if __name__ == "__main__":
    run_consumer()