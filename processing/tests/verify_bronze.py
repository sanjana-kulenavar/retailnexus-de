# processing/tests/verify_bronze.py
# Run this to verify your Bronze data looks correct after Phase 2

import os
import sys

# Add processing/spark_jobs to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)
)))
sys.path.insert(0, os.path.join(project_root, "processing", "spark_jobs"))

from utils.spark_session import get_spark_session
from pyspark.sql import functions as F
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE", "retailnexus-bronze-dev")
BRONZE_PATH = f"s3a://{S3_BUCKET_BRONZE}/sales/"


def verify_bronze():
    """
    Reads all Bronze Parquet data from S3 and prints a quality report.

    Checks:
    1. Total row count
    2. Null values in critical columns (should be 0)
    3. Audit flag distribution (~98% VALID expected)
    4. Payment method breakdown (Dutch market proportions)
    5. Top 10 stores by revenue
    6. Five sample records
    """

    spark = get_spark_session("verify-bronze")

    print("\n" + "=" * 60)
    print("  Bronze Data Verification Report — RetailNexus DE")
    print("=" * 60)

    # Read all Parquet files under sales/ prefix
    # Spark discovers all date and store partitions automatically
    df = spark.read.parquet(BRONZE_PATH)

    # ── 1. Total row count ────────────────────────────────────────────
    total_rows = df.count()
    print(f"\n  Total transactions in Bronze: {total_rows:,}")

    if total_rows == 0:
        print("  ❌ ERROR: No data found. Is the consumer running?")
        spark.stop()
        return

    # ── 2. Null check on critical columns ────────────────────────────
    # In Oracle ReSA: null transaction IDs caused processing failures
    # We verify there are zero nulls in our three most critical fields
    print("\n  Null check (all should be 0):")
    null_check = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in ["transaction_id", "store_id", "total_amount"]
    ])
    null_check.show()

    # ── 3. Audit flag distribution ────────────────────────────────────
    # Expected: ~98% VALID, ~2% VOIDED, tiny % EXCEPTION_HIGH_VALUE
    # This mirrors what you would see in Oracle ReSA audit reports
    print("  Audit flag distribution (mirrors ReSA audit categories):")
    df.groupBy("audit_flag") \
      .count() \
      .withColumn(
          "percentage",
          F.round(F.col("count") / total_rows * 100, 2)
      ) \
      .orderBy("count", ascending=False) \
      .show()

    # ── 4. Payment method breakdown ───────────────────────────────────
    # Expected: CONTACTLESS ~60%, CARD ~25%, CASH ~12%, VOUCHER ~3%
    print("  Payment method breakdown (Dutch market proportions):")
    df.groupBy("payment_method") \
      .count() \
      .withColumn(
          "percentage",
          F.round(F.col("count") / total_rows * 100, 2)
      ) \
      .orderBy("count", ascending=False) \
      .show()

    # ── 5. Top 10 stores by revenue ───────────────────────────────────
    print("  Top 10 stores by total revenue (VALID transactions only):")
    df.filter(F.col("audit_flag") == "VALID") \
      .groupBy("store_id") \
      .agg(
          F.count("*").alias("transactions"),
          F.round(F.sum("total_amount"), 2).alias("revenue_eur"),
          F.round(F.avg("total_amount"), 2).alias("avg_basket_eur")
      ) \
      .orderBy("revenue_eur", ascending=False) \
      .limit(10) \
      .show()

    # ── 6. Five sample records ────────────────────────────────────────
    print("  Sample records from Bronze:")
    df.select(
        "transaction_id", "store_id", "product_id",
        "quantity", "unit_price", "total_amount",
        "payment_method", "audit_flag"
    ).limit(5).show(truncate=False)

    print("=" * 60)
    print("  ✅ Bronze verification complete")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    verify_bronze()