-- Light cleanup of raw.orders: drop ingestion-metadata columns
-- (file_name, file_size, read_timestamp) that don't belong in analytics.
-- Note: the raw column is literally called product_code, but it was only
-- ever a renamed copy of the original product_id (see run_orders_pipeline.py
-- upstream). We rename it back here to avoid confusing it with the
-- name-derived hash that products/pricing call product_code.
select
    order_id,
    order_placement_date,
    customer_id,
    product_code as product_id,
    order_qty
from {{ source('raw', 'orders') }}
 