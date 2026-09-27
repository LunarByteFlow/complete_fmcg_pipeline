-- Light passthrough of raw.pricing (already deduplicated to one row
-- per product_code/year back in the Gold layer in Databricks).
select
    product_id,
    product_code,  -- name-derived hash; kept for reference, not used as a join key
    year,
    price_inr
from {{ source('raw', 'pricing') }}