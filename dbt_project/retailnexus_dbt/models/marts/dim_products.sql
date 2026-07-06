{{ config(materialized='table') }}

with products as (
    select
        product_id,
        count(*)                         as times_sold,
        round(avg(unit_price), 2)        as avg_price,
        max(unit_price)                  as max_price,
        min(unit_price)                  as min_price
    from {{ ref('stg_sales_transactions') }}
    group by product_id
)

select * from products
