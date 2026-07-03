import os
import sys

sys.path.insert(0, os.path.join(os.getcwd(), "processing", "spark_jobs"))

from utils.spark_session import get_spark_session
from utils.delta_helpers import read_delta
from pyspark.sql import functions as F
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "retailnexus-silver-dev")
SILVER_PATH = f"s3a://{S3_BUCKET_SILVER}/sales_transactions/"


def verify_silver():
    spark = get_spark_session("verify-silver")
    print("\n" + "=" * 60)
    print("  Silver Data Verification Report")
    print("=" * 60)

    df = read_delta(spark, SILVER_PATH)
    total = df.count()
    print(f"\n  Total records in Silver: {total:,}")

    if total == 0:
        print("  ERROR: No data in Silver.")
        spark.stop()
        return

    distinct_ids = df.select("transaction_id").distinct().count()
    print(f"\n  Duplicate check (should be 0): {total - distinct_ids}")

    mismatches = df.filter(
        F.abs(F.col("total_amount") - F.round(F.col("quantity") * F.col("unit_price"), 2)) > 0.01
    ).count()
    print(f"  Total mismatches (should be 0): {mismatches}")

    negatives = df.filter(F.col("total_amount") < 0).count()
    print(f"  Negative amounts (should be 0): {negatives}")

    print("\n  Audit flag distribution:")
    df.groupBy("audit_flag").count() \
      .withColumn("percentage", F.round(F.col("count") / total * 100, 2)) \
      .orderBy("count", ascending=False).show()

    print("  Sample Silver records:")
    df.select("transaction_id", "store_id", "total_amount", "audit_flag", "data_layer") \
      .limit(5).show(truncate=False)

    print("=" * 60)
    print("  Silver verification complete")
    print("=" * 60 + "\n")
    spark.stop()


if __name__ == "__main__":
    verify_silver()
