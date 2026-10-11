-- The customer total must equal subtotal + delivery fee + tip (to the cent).
select order_id, subtotal, delivery_fee, tip, gross_amount
from {{ ref('fact_orders') }}
where abs(subtotal + delivery_fee + tip - gross_amount) > 0.01
