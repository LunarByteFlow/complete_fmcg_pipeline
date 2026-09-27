-- One row per product -- the "what" dimension.
select distinct
    product_id,
    product_code,
    product_name,
    category,
    division
from {{ ref('stg_products') }}