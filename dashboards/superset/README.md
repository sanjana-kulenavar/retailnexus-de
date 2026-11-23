# Connecting Superset to Snowflake (Green-layer dashboard)

Once Superset is running (see Phase 6 of the phase guides), connect it to the
Snowflake **MARTS** schema and build the audit dashboard:

1. **Settings → Database Connections → + Database → Snowflake**
2. Account: `ihekpml-ac28354` (from Phase 4)
3. Database: `RETAILNEXUS_PROD`
4. Schema: `MARTS`
5. Role: `DBT_ROLE`
6. Warehouse: `RETAILNEXUS_WH`
7. Test connection, then connect.

## Datasets

Create datasets on:

- `FCT_DAILY_SALES` (metrics: `TRANSACTION_COUNT`, `VALID_REVENUE`, `VOIDED_COUNT`, `AVG_TRANSACTION_VALUE`)
- `DIM_STORES`
- `DIM_PRODUCTS`

## Charts

| Chart | Dataset | Chart type | X | Metrics |
|---|---|---|---|---|
| Revenue by Store | FCT_DAILY_SALES | Bar | STORE_ID | `SUM(VALID_REVENUE)` |
| Transactions vs Voids | FCT_DAILY_SALES | Bar | STORE_ID | `SUM(TRANSACTION_COUNT)`, `SUM(VOIDED_COUNT)` |
| Avg Basket Value | FCT_DAILY_SALES | Big Number | - | `AVG(AVG_TRANSACTION_VALUE)` |
| Top Products by Volume | DIM_PRODUCTS | Table | PRODUCT_ID | `TIMES_SOLD` desc |

## Dashboard

Dashboard title: **RetailNexus ReSA — Sales Audit Dashboard**.

## What's missing to run this live

- Snowflake account credentials (account `ihekpml-ac28354`, username, and a
  password) — the creds file `retailnexus-de-user_credentials.csv` next to
  this folder is AWS, not Snowflake.
- Data loaded into Snowflake `RETAILNEXUS_PROD.RAW.SALES_TRANSACTIONS_RAW`
  and `RETAILNEXUS_PROD.MARTS` built by `dbt run`.
- A running Superset instance.
