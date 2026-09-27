
-- One row per customer -- the "who" dimension.
select distinct
    customer_id,
    customer_name,
    city,
    market
from {{ ref('stg_customers') }}
 
