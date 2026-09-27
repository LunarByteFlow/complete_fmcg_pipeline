-- One row per order -- narrow by design: keys + numeric measures only.
-- No customer/product descriptive text lives here (that's what the
-- dimension tables are for) -- just enough to join out to them, plus
-- order_qty and the new revenue figure.
with orders as (
    select * from {{ ref('stg_orders') }}
),

pricing as (
    select * from {{ ref('stg_pricing') }}
)

select
    -- Surrogate key: the true grain of this table is one row per order
    -- LINE (an order_id can span multiple products), not one row per order.
    o.order_id || '-' || o.product_id as order_line_id,
    o.order_id,
    o.order_placement_date,
    o.customer_id,
    o.product_id,
    o.order_qty,
    p.price_inr,
    o.order_qty * p.price_inr as revenue
from orders o
left join pricing p
    on o.product_id = p.product_id
    and cast(year(o.order_placement_date) as string) = p.year
