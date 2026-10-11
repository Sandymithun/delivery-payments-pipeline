with source as (

    select * from {{ source('raw', 'orders') }}

),

deduped as (

    -- a re-delivered file must never create a second copy of an order
    select *
    from source
    qualify row_number() over (partition by order_id order by _loaded_at desc) = 1

)

select
    order_id,
    customer_id,
    merchant_id,
    cast(order_ts as timestamp)                  as order_ts,
    cast(cast(order_ts as timestamp) as date)    as order_date,
    lower(status)                                as order_status,
    cast(items_count as integer)                 as items_count,
    cast(subtotal as decimal(12, 2))             as subtotal,
    cast(delivery_fee as decimal(12, 2))         as delivery_fee,
    cast(tip as decimal(12, 2))                  as tip,
    cast(total_amount as decimal(12, 2))         as total_amount,
    upper(currency)                              as currency,
    try_cast(delivery_minutes as integer)        as delivery_minutes,
    _batch_date                                  as batch_date
from deduped
