# Dashboards

Superset-based serving layer. See [superset/README.md](superset/README.md)
for the connection details, datasets, and chart specifications.

## Planned scope

Once Silver data is loaded into Snowflake RAW and `dbt run` has built the Gold
tables, Apache Superset will connect to the `MARTS` schema and expose:

| Chart | Dataset | Metric |
|---|---|---|
| Revenue by store | `FCT_DAILY_SALES` | `SUM(valid_revenue)` |
| Transactions vs voids | `FCT_DAILY_SALES` | `SUM(transaction_count)`, `SUM(voided_count)` |
| Average basket value | `FCT_DAILY_SALES` | `AVG(avg_transaction_value)` |
| Top products by volume | `DIM_PRODUCTS` | `times_sold` |

Dashboard title: **RetailNexus ReSA — Sales Audit Dashboard**.

## Connection requirements

- Snowflake database `RETAILNEXUS_PROD`, schema `MARTS`
- A role with `USAGE` on the warehouse and read access to the marts
- The `snowflake-sqlalchemy` driver installed in the Superset image

## Status

| Item | Status |
|---|---|
| Superset instance | Not started |
| Snowflake connection | Not started |
| Charts | Not started |
| Assembled dashboard | Not started |
| Dashboard screenshot | Not started |
