"""
Load Silver data into Snowflake RETAILNEXUS_PROD.RAW.SALES_TRANSACTIONS_RAW.

Prerequisites (per Phase 4, once the Snowflake account is active):
- Objects already created (see Phase 4 Part C3):
  WAREHOUSE RETAILNEXUS_WH, DATABASE RETAILNEXUS_PROD, ROLE DBT_ROLE,
  SCHEMAS RAW/STAGING/MARTS.
- Silver data exported to silver_export.csv (e.g. via
  processing/tests/export_silver_csv.py).

Usage (from the project root):
    .venv/bin/python dashboards/superset/load_silver_to_snowflake.py [csv_path]
"""
import sys
from pathlib import Path

import pandas as pd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

COLUMNS = [
    "transaction_id",
    "store_id",
    "terminal_id",
    "cashier_id",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
    "payment_method",
    "transaction_ts",
    "is_voided",
    "audit_flag",
    "year",
    "month",
    "day",
]


def main() -> int:
    csv_path = Path(sys.argv[1] if len(sys.argv) > 1 else "silver_export.csv")
    df = pd.read_csv(csv_path)[COLUMNS]

    conn = connect(
        account="ihekpml-ac28354",
        user="SANJANA",
        password="Rahul@october2026",
        role="DBT_ROLE",
        warehouse="RETAILNEXUS_WH",
        database="RETAILNEXUS_PROD",
        schema="RAW",
    )
    try:
        written, _, _ = write_pandas(conn, df, "SALES_TRANSACTIONS_RAW")
        print(f"Loaded {written} rows into RETAILNEXUS_PROD.RAW.SALES_TRANSACTIONS_RAW")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
