-- Collapse the payment event stream to one row per order.
with payments as (

    select * from {{ ref('stg_payments') }}

)

select
    order_id,
    sum(case when payment_status = 'captured' then amount else 0 end)            as captured_amount,
    -sum(case when payment_status = 'refunded' then amount else 0 end)           as refunded_amount,
    sum(case when payment_status in ('captured', 'refunded') then amount else 0 end) as net_paid_amount,
    sum(case when payment_status = 'failed' then 1 else 0 end)                   as failed_attempts,
    min(case when payment_status = 'captured' then payment_ts end)               as captured_at,
    -- method of the successful capture; fall back to whatever was attempted
    coalesce(
        max(case when payment_status = 'captured' then payment_method end),
        max(payment_method)
    )                                                                            as payment_method,
    max(batch_date)                                                              as last_payment_batch_date
from payments
group by order_id
