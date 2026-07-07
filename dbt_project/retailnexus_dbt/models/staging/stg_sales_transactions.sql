-- Staging: clean and standardize the raw sales data

with source as (
    select * from {{ source('raw', 'sales_transactions_raw') }}
),

cleaned as (
    select
        transaction_id,
        store_id,
        terminal_id,
        cashier_id,
        product_id,
        cast(quantity as integer)         as quantity,
        cast(unit_price as float)         as unit_price,
        cast(total_amount as float)       as total_amount,
        payment_method,
        cast(transaction_ts as timestamp) as transaction_ts,
        is_voided,
        audit_flag,
        cast(year as integer)             as year,
        cast(month as integer)            as month,
        cast(day as integer)              as day
    from source
    where transaction_id is not null
)

select * from cleaned
