import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(project_root, "processing", "spark_jobs"))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.spark_session import get_spark_session
from utils.delta_helpers import write_delta
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE", "retailnexus-bronze-dev")
S3_BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "retailnexus-silver-dev")

BRONZE_PATH = f"s3a://{S3_BUCKET_BRONZE}/sales/"
SILVER_PATH = f"s3a://{S3_BUCKET_SILVER}/sales_transactions/"
REJECTS_PATH = f"s3a://{S3_BUCKET_SILVER}/sales_rejects/"


def transform():
    print("=" * 60)
    print("  ReSA Bronze -> Silver Transformation")
    print("=" * 60)

    spark = get_spark_session("resa-bronze-to-silver")

    print("\n  [1/6] Reading Bronze data...")
    bronze_df = spark.read.parquet(BRONZE_PATH)
    bronze_count = bronze_df.count()
    print(f"        Bronze records read: {bronze_count:,}")

    if bronze_count == 0:
        print("  ERROR: No Bronze data found.")
        spark.stop()
        return

    print("  [2/6] Deduplicating by transaction_id...")
    window = Window.partitionBy("transaction_id").orderBy(F.col("ingested_at").desc())
    deduped_df = (
        bronze_df
        .withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    deduped_count = deduped_df.count()
    duplicates_removed = bronze_count - deduped_count
    print(f"        Duplicates removed: {duplicates_removed:,}")

    print("  [3/6] Cleaning and type casting...")
    cleaned_df = (
        deduped_df
        .withColumn("transaction_ts", F.to_timestamp("transaction_ts"))
        .withColumn("expected_total", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn("data_layer", F.lit("SILVER"))
    )

    print("  [4/6] Applying business rule validation...")
    validated_df = (
        cleaned_df
        .withColumn(
            "rejection_reason",
            F.when(F.col("transaction_id").isNull(), F.lit("NULL_TRANSACTION_ID"))
             .when(F.col("store_id").isNull(), F.lit("NULL_STORE_ID"))
             .when(F.col("total_amount") < 0, F.lit("NEGATIVE_AMOUNT"))
             .when(F.abs(F.col("total_amount") - F.col("expected_total")) > 0.01, F.lit("TOTAL_MISMATCH"))
             .when(~F.col("store_id").rlike("^NL-[0-9]{3}$"), F.lit("INVALID_STORE_FORMAT"))
             .otherwise(F.lit(None))
        )
        .withColumn("is_valid", F.col("rejection_reason").isNull())
    )

    print("  [5/6] Re-classifying audit flags...")
    final_df = validated_df.withColumn(
        "audit_flag",
        F.when(F.col("is_voided"), F.lit("VOIDED"))
         .when(F.col("total_amount") > 5000, F.lit("EXCEPTION_HIGH_VALUE"))
         .otherwise(F.lit("VALID"))
    )

    print("  [6/6] Writing to Delta Lake...")
    valid_df = final_df.filter(F.col("is_valid")).drop("expected_total", "is_valid", "rejection_reason")
    valid_count = valid_df.count()
    write_delta(valid_df, SILVER_PATH, mode="overwrite", partition_by=["year", "month", "day"])
    print(f"        Valid records -> Silver: {valid_count:,}")

    rejects_df = final_df.filter(~F.col("is_valid")).select(
        "transaction_id", "store_id", "product_id", "quantity", "unit_price",
        "total_amount", "expected_total", "rejection_reason", "silver_processed_at"
    )
    rejects_count = rejects_df.count()
    if rejects_count > 0:
        write_delta(rejects_df, REJECTS_PATH, mode="overwrite")
        print(f"        Invalid records -> Rejects: {rejects_count:,}")
    else:
        print("        No invalid records found.")

    print("\n" + "=" * 60)
    print("  Transformation Summary")
    print("=" * 60)
    print(f"  Bronze read       : {bronze_count:,}")
    print(f"  Duplicates removed: {duplicates_removed:,}")
    print(f"  Valid -> Silver   : {valid_count:,}")
    print(f"  Invalid -> Rejects: {rejects_count:,}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    transform()
