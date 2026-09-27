CREATE WAREHOUSE compute_wh WITH WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE; 
CREATE DATABASE fmcg_analytics; 
CREATE SCHEMA fmcg_analytics.raw;


ALTER USER MAHNOOR SET DEFAULT_NAMESPACE = 'FMCG_ANALYTICS.RAW';

CREATE STAGE fmcg_analytics.raw.s3_stage
  URL='s3://sportsbar-db-484395054876-us-east-1-an/snowflake_export/'
  CREDENTIALS = (AWS_KEY_ID='key ' AWS_SECRET_KEY='key');

  LIST @fmcg_analytics.raw.s3_stage;


LIST @fmcg_analytics.raw.s3_stage/orders/;
LIST @fmcg_analytics.raw.s3_stage/orders/;

SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ORDERS',
    START_TIME => DATEADD(HOURS, -6, CURRENT_TIMESTAMP())
));

  USE DATABASE fmcg_analytics;
USE SCHEMA raw;

TRUNCATE TABLE fmcg_analytics.raw.orders;
TRUNCATE TABLE orders;

-- ORDERS
CREATE OR REPLACE TABLE orders (
    order_id VARCHAR,
    order_placement_date DATE,
    customer_id VARCHAR,
    product_code VARCHAR,
    order_qty DOUBLE,
    read_timestamp TIMESTAMP_NTZ,
    file_name VARCHAR,
    file_size NUMBER
);



COPY INTO orders
FROM @fmcg_analytics.raw.s3_stage/orders/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
FORCE = TRUE;

SELECT COUNT(*) FROM orders;

DROP TABLE fmcg_analytics.public.orders;

-- CUSTOMERS
CREATE OR REPLACE TABLE customers (
    customer_id VARCHAR,
    customer_name VARCHAR,
    city VARCHAR,
    read_timestamp TIMESTAMP_NTZ,
    file_name VARCHAR,
    file_size NUMBER,
    customer VARCHAR,
    market VARCHAR,
    platform VARCHAR,
    channel VARCHAR
);

COPY INTO customers
FROM @s3_stage/customers/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- PRODUCTS
CREATE OR REPLACE TABLE products (
    product_code VARCHAR,
    division VARCHAR,
    category VARCHAR,
    product VARCHAR,
    variant VARCHAR,
    product_id VARCHAR,
    read_timestamp TIMESTAMP_NTZ,
    file_name VARCHAR,
    file_size NUMBER
);

COPY INTO products
FROM @s3_stage/products/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE OR REPLACE TABLE pricing (
    product_code VARCHAR,
    price_inr DOUBLE,
    year VARCHAR
);

COPY INTO pricing
FROM @s3_stage/pricing/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet';

SELECT COUNT(*) FROM pricing;

CREATE OR REPLACE FILE FORMAT fmcg_analytics.raw.parquet_fmt
  TYPE = PARQUET;

-- Now we will be adding data into existing tables 
TRUNCATE TABLE orders;
TRUNCATE TABLE customers;
TRUNCATE TABLE products;



COPY INTO customers
FROM @s3_stage/customers/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet';

COPY INTO products
FROM @s3_stage/products/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'



SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ORDERS',
    START_TIME => DATEADD(HOURS, -3, CURRENT_TIMESTAMP())
));


SELECT * FROM staging_marts.fact_orders LIMIT 10;

SELECT DISTINCT year FROM fmcg_analytics.raw.pricing ORDER BY year;

SELECT DISTINCT product_code FROM fmcg_analytics.raw.pricing LIMIT 20;



SELECT
    o.product_code,
    cast(year(o.order_placement_date) as string) as order_year,
    p.product_code as pricing_product_code,
    p.year as pricing_year,
    p.price_inr
FROM staging_staging.stg_orders o
LEFT JOIN staging_staging.stg_pricing p
    ON o.product_code = p.product_code
    AND cast(year(o.order_placement_date) as string) = p.year
LIMIT 10;

SELECT * FROM staging_staging.stg_pricing LIMIT 20;

DESCRIBE TABLE fmcg_analytics.raw.pricing;

CREATE OR REPLACE TABLE fmcg_analytics.raw.pricing (
    product_id VARCHAR,
    product_code VARCHAR,
    price_inr DOUBLE,
    year VARCHAR
);

COPY INTO fmcg_analytics.raw.pricing
FROM @fmcg_analytics.raw.s3_stage/pricing/


SELECT
    COUNT(*) AS total_orders,
    COUNT(price_inr) AS orders_with_price,
    ROUND(COUNT(price_inr) * 100.0 / COUNT(*), 1) AS match_rate_pct
FROM staging_marts.fact_orders;
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet';

DESCRIBE TABLE fmcg_analytics.raw.pricing;

SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM fmcg_analytics.staging.stg_orders;
SELECT COUNT(*) FROM fmcg_analytics.marts.fact_orders;

SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_USER();

SHOW TABLES LIKE 'ORDERS' IN DATABASE FMCG_ANALYTICS;