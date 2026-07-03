import os
import sys

sys.path.insert(0, os.path.join(os.getcwd(), "processing", "spark_jobs"))

from utils.spark_session import get_spark_session
from delta.tables import DeltaTable
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "retailnexus-silver-dev")
SILVER_PATH = f"s3a://{S3_BUCKET_SILVER}/sales_transactions/"


def show_history():
    spark = get_spark_session("delta-history")
    print("\n" + "=" * 60)
    print("  Delta Table Version History (Time Travel Demo)")
    print("=" * 60)

    delta_table = DeltaTable.forPath(spark, SILVER_PATH)

    print("\n  Every version of this table:")
    delta_table.history().select("version", "timestamp", "operation").show(truncate=False)

    print("  Reading VERSION 0 (first write):")
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(SILVER_PATH)
    print(f"        Version 0 row count: {v0.count():,}")

    print("  Reading CURRENT version:")
    current = spark.read.format("delta").load(SILVER_PATH)
    print(f"        Current row count: {current.count():,}")

    print("\n  You just queried a past version — impossible with plain Parquet.")
    print("=" * 60 + "\n")
    spark.stop()


if __name__ == "__main__":
    show_history()
