-- Completeness: every distinct order in raw must reach the fact table exactly once.
with raw_orders as (
    select count(distinct order_id) as n from {{ source('raw', 'orders') }}
),
fact as (
    select count(*) as n from {{ ref('fact_orders') }}
)
select raw_orders.n as raw_orders, fact.n as fact_orders
from raw_orders cross join fact
where raw_orders.n <> fact.n
