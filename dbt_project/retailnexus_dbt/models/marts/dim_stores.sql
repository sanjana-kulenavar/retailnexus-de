{{ config(materialized='table') }}

with stores as (
    select distinct
        store_id,
        'Netherlands'                    as country,
        substring(store_id, 4, 3)        as store_number
    from {{ ref('stg_sales_transactions') }}
)

select * from stores
