import os
import sys

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
    spark = get_spark_session("verify-bronze")
    print("\n" + "=" * 60)
    print("  Bronze Data Verification Report")
    print("=" * 60)

    df = spark.read.parquet(BRONZE_PATH)
    total_rows = df.count()
    print(f"\n  Total transactions in Bronze: {total_rows:,}")

    if total_rows == 0:
        print("  ERROR: No data found.")
        spark.stop()
        return

    print("\n  Null check (all should be 0):")
    df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in ["transaction_id", "store_id", "total_amount"]
    ]).show()

    print("  Audit flag distribution:")
    df.groupBy("audit_flag").count() \
      .withColumn("percentage", F.round(F.col("count") / total_rows * 100, 2)) \
      .orderBy("count", ascending=False).show()

    print("  Payment method breakdown:")
    df.groupBy("payment_method").count() \
      .withColumn("percentage", F.round(F.col("count") / total_rows * 100, 2)) \
      .orderBy("count", ascending=False).show()

    print("  Top 10 stores by revenue (VALID only):")
    df.filter(F.col("audit_flag") == "VALID") \
      .groupBy("store_id") \
      .agg(
          F.count("*").alias("transactions"),
          F.round(F.sum("total_amount"), 2).alias("revenue_eur"),
          F.round(F.avg("total_amount"), 2).alias("avg_basket_eur")
      ).orderBy("revenue_eur", ascending=False).limit(10).show()

    print("  Sample records:")
    df.select(
        "transaction_id", "store_id", "product_id",
        "quantity", "unit_price", "total_amount",
        "payment_method", "audit_flag"
    ).limit(5).show(truncate=False)

    print("=" * 60)
    print("  Bronze verification complete")
    print("=" * 60 + "\n")
    spark.stop()


if __name__ == "__main__":
    verify_bronze()
