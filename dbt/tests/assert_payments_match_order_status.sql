-- Money must agree with what happened to the order:
--   delivered -> captured in full, nothing refunded
--   refunded  -> captured then refunded in full (net 0)
--   cancelled -> nothing captured
select order_id, order_status, gross_amount, captured_amount, refunded_amount, net_paid_amount
from {{ ref('fact_orders') }}
where
    (order_status = 'delivered' and (abs(captured_amount - gross_amount) > 0.01 or refunded_amount <> 0))
 or (order_status = 'refunded'  and (abs(captured_amount - gross_amount) > 0.01 or net_paid_amount <> 0))
 or (order_status = 'cancelled' and captured_amount <> 0)
