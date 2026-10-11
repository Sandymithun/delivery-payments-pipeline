{{ config(materialized='table') }}

-- Type 1 customer dimension (latest attributes overwrite older ones).
with customers as (

    select * from {{ ref('stg_customers') }}

),

first_orders as (

    select customer_id, min(order_date) as first_order_date
    from {{ ref('stg_orders') }}
    group by customer_id

)

select
    {{ surrogate_key(['c.customer_id']) }}               as customer_key,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name                    as full_name,
    c.email,
    c.city,
    c.signup_date,
    {{ dbt.date_trunc('month', 'c.signup_date') }}        as signup_month,
    f.first_order_date,
    f.first_order_date is not null                        as has_ordered
from customers c
left join first_orders f using (customer_id)
