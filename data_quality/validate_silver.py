# data_quality/validate_silver.py
# Great Expectations 1.0 validation of the Silver Delta table

import os
import sys

sys.path.insert(0, os.path.join(os.getcwd(), "processing", "spark_jobs"))

import great_expectations as gx
from utils.spark_session import get_spark_session
from utils.delta_helpers import read_delta
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "retailnexus-silver-dev")
SILVER_PATH = f"s3a://{S3_BUCKET_SILVER}/sales_transactions/"


def validate_silver():
    print("=" * 60)
    print("  Silver Data Quality Validation (Great Expectations)")
    print("=" * 60)

    # ── Step 1: Read Silver into a Spark DataFrame ──────────────
    spark = get_spark_session("gx-validate-silver")
    df = read_delta(spark, SILVER_PATH)
    row_count = df.count()
    print(f"\n  Loaded Silver table: {row_count:,} rows")

    if row_count == 0:
        print("  ERROR: Silver table is empty.")
        spark.stop()
        return

    # ── Step 2: Create a GX Data Context ────────────────────────
    # "ephemeral" mode keeps everything in memory (no files created).
    # Simple and clean for a validation run.
    context = gx.get_context(mode="ephemeral")

    # ── Step 3: Register a Spark Data Source and Asset ──────────
    data_source = context.data_sources.add_spark(
        name="silver_spark_source",
        force_reuse_spark_context=True,
    )
    data_asset = data_source.add_dataframe_asset(name="silver_sales")

    # ── Step 4: Create a Batch Definition (whole DataFrame) ─────
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "silver_batch"
    )

    # ── Step 5: Create an Expectation Suite ─────────────────────
    suite = gx.ExpectationSuite(name="silver_sales_quality")
    suite = context.suites.add(suite)

    # ── Step 6: Add expectations (your data quality rules) ──────
    # Rule 1: transaction_id must never be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="transaction_id")
    )
    # Rule 2: store_id must never be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="store_id")
    )
    # Rule 3: total_amount must be between 0 and 100,000
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="total_amount", min_value=0, max_value=100000
        )
    )
    # Rule 4: quantity must be at least 1
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="quantity", min_value=1, max_value=100
        )
    )
    # Rule 5: audit_flag must only contain these three values
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="audit_flag",
            value_set=["VALID", "VOIDED", "EXCEPTION_HIGH_VALUE"],
        )
    )
    # Rule 6: payment_method must only contain these four values
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="payment_method",
            value_set=["CARD", "CASH", "CONTACTLESS", "VOUCHER"],
        )
    )
    # Rule 7: store_id must match the NL-### pattern
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="store_id", regex=r"^NL-[0-9]{3}$"
        )
    )
    # Rule 8: the table must have at least 1 row
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
    )

    # ── Step 7: Create a Validation Definition ──────────────────
    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name="silver_validation",
    )
    validation_definition = context.validation_definitions.add(
        validation_definition
    )

    # ── Step 8: Run the validation ──────────────────────────────
    print("\n  Running validation...")
    results = validation_definition.run(
        batch_parameters={"dataframe": df}
    )

    # ── Step 9: Print a readable report ─────────────────────────
    print("\n" + "=" * 60)
    print("  Validation Report")
    print("=" * 60)

    overall = "PASSED" if results.success else "FAILED"
    print(f"\n  Overall result: {overall}\n")

    for r in results.results:
        exp_type = r.expectation_config.type
        col = r.expectation_config.kwargs.get("column", "(table)")
        status = "PASS" if r.success else "FAIL"
        unexpected = r.result.get("unexpected_count", 0)
        print(f"  [{status}] {exp_type} on '{col}'"
              + (f"  ({unexpected} bad values)" if not r.success else ""))

    print("\n" + "=" * 60)
    if results.success:
        print("  All data quality expectations passed.")
    else:
        print("  Some expectations FAILED — investigate above.")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    validate_silver()
