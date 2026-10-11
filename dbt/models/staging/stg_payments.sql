with source as (

    select * from {{ source('raw', 'payments') }}

),

deduped as (

    -- gateway webhook retries send the same payment_id more than once
    select *
    from source
    qualify row_number() over (partition by payment_id order by _loaded_at desc) = 1

)

select
    payment_id,
    order_id,
    cast(payment_ts as timestamp)        as payment_ts,
    lower(method)                        as payment_method,
    cast(amount as decimal(12, 2))       as amount,
    upper(currency)                      as currency,
    lower(status)                        as payment_status,
    _batch_date                          as batch_date
from deduped
