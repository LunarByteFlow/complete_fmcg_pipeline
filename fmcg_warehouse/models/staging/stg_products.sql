-- Light cleanup of raw.products: keep the descriptive attributes,
-- rename product -> product_name for clarity downstream.
select
    product_id,
    product_code,  -- name-derived hash; kept for reference, not used as a join key
    product as product_name,
    category,
    division
from {{ source('raw', 'products') }}
 