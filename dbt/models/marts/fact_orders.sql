{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

-- Grain: one row per order.
with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('int_order_payments') }}

),

{% if is_incremental() %}
watermark as (
    -- re-process the last N loaded days so late payment events (refunds) land
    select {{ dbt.dateadd('day', -1 * var('lookback_days'), 'max(batch_date)') }} as since
    from {{ this }}
),
{% endif %}

merchants as (

    select * from {{ ref('dim_merchant') }}

)

select
    o.order_id,
    {{ surrogate_key(['o.customer_id']) }}                    as customer_key,
    m.merchant_key,
    {{ date_key('o.order_date') }}                            as order_date_key,
    o.order_ts,
    o.order_status,
    o.items_count,

    -- order amounts (what the customer was charged)
    o.subtotal,
    o.delivery_fee,
    o.tip,
    o.total_amount                                            as gross_amount,

    -- payment outcome
    coalesce(p.captured_amount, 0)                            as captured_amount,
    coalesce(p.refunded_amount, 0)                            as refunded_amount,
    coalesce(p.net_paid_amount, 0)                            as net_paid_amount,
    coalesce(p.failed_attempts, 0)                            as failed_payment_attempts,
    p.payment_method,
    p.captured_at,

    -- platform economics, using the commission valid at order time (SCD2)
    m.commission_rate,
    case when o.order_status = 'delivered'
         then cast(o.subtotal * m.commission_rate as decimal(12, 2)) else 0 end
                                                              as commission_amount,
    case when o.order_status = 'delivered'
         then cast(o.subtotal * m.commission_rate + o.delivery_fee as decimal(12, 2)) else 0 end
                                                              as platform_revenue,

    o.delivery_minutes,
    o.order_status = 'delivered'                              as is_delivered,
    o.batch_date

from orders o
left join payments p
    on o.order_id = p.order_id
left join merchants m
    on  o.merchant_id = m.merchant_id
    and o.order_ts >= m.valid_from
    and (m.valid_to is null or o.order_ts < m.valid_to)

{% if is_incremental() %}
where o.batch_date >= (select since from watermark)
   or p.last_payment_batch_date >= (select since from watermark)
{% endif %}
