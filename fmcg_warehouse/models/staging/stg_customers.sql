
-- Light cleanup of raw.customers: keep only real business columns,
-- drop lakehouse audit fields and the hardcoded platform/channel labels.
select
    customer_id,
    customer_name,
    city,
    market
from {{ source('raw', 'customers') }}
 
