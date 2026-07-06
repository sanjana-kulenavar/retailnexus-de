-- Fact table: daily sales aggregated by store
-- This is your ReSA daily audit summary, in dimensional form

{{ config(materialized='table') }}

with sales as (
    select * from {{ ref('stg_sales_transactions') }}
),

daily_agg as (
    select
        store_id,
        year,
        month,
        day,
        count(*)                                   as transaction_count,
        sum(case when audit_flag = 'VALID'
            then total_amount else 0 end)          as valid_revenue,
        sum(case when audit_flag = 'VOIDED'
            then 1 else 0 end)                     as voided_count,
        round(avg(total_amount), 2)                as avg_transaction_value
    from sales
    group by store_id, year, month, day
)

select * from daily_agg
