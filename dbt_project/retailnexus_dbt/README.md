# retailnexus_dbt

dbt project for the RetailNexus DE Gold layer. It reads the Silver data loaded
into `RETAILNEXUS_PROD.RAW.SALES_TRANSACTIONS_RAW` and builds the daily sales
fact and store/product dimensions in Snowflake.

## Models

| Layer | Model | Materialization |
|---|---|---|
| Staging | `stg_sales_transactions` | view (default) |
| Marts | `fct_daily_sales` | table |
| Marts | `dim_stores` | table |
| Marts | `dim_products` | table |

## Profile

The project uses the `retailnexus_dbt` profile. Copy the example and supply
credentials through environment variables:

```bash
mkdir -p profiles
cp profiles/profiles.yml.example profiles/profiles.yml
```

Required environment variables: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`,
`SNOWFLAKE_PASSWORD`, and optionally `SNOWFLAKE_ROLE`, `SNOWFLAKE_DATABASE`,
`SNOWFLAKE_WAREHOUSE`, and `SNOWFLAKE_SCHEMA`.

## Commands

```bash
dbt debug --profiles-dir profiles
dbt run   --profiles-dir profiles
dbt test  --profiles-dir profiles
dbt docs generate --profiles-dir profiles
dbt docs serve
```

## Tests

Eleven tests are declared: uniqueness and non-null checks on the staging and
dimension keys, non-null checks on fact measures, and a referential-integrity
check from `fct_daily_sales.store_id` to `dim_stores.store_id`.
