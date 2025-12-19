-- 03_ref_transformations.sql
USE ROLE SYSADMIN;
USE DATABASE RETAIL_DB;
USE WAREHOUSE WH_RETAIL;

-- Country -> Region map
CREATE OR REPLACE TABLE RETAIL_DB.REF.COUNTRY_REGION_MAP (
  country STRING,
  region  STRING
);

INSERT OVERWRITE INTO RETAIL_DB.REF.COUNTRY_REGION_MAP VALUES
  ('FR','EU'), ('ES','EU'), ('DE','EU'), ('IT','EU'), ('NL','EU'), ('BE','EU'), ('GB','EU'),
  ('US','AMER'), ('CA','AMER'),
  ('AU','APAC');

-- REF.SALES_TX (typed + dedup)
CREATE OR REPLACE TABLE RETAIL_DB.REF.SALES_TX AS
SELECT
  r.order_id::NUMBER              AS order_id,
  r.order_ts::TIMESTAMP           AS order_ts,
  r.customer_id::STRING           AS customer_id,
  r.email::STRING                 AS email,
  r.country::STRING               AS country,
  m.region::STRING                AS region,
  r.amount::NUMBER(10,2)          AS amount,
  r.currency::STRING              AS currency,
  r.payment_type::STRING          AS payment_type,
  r._loaded_at                    AS _loaded_at,
  r._file_name                    AS _file_name
FROM RETAIL_DB.RAW.SALES_TX_RAW r
LEFT JOIN RETAIL_DB.REF.COUNTRY_REGION_MAP m
  ON r.country = m.country
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY r.order_id
  ORDER BY r._loaded_at DESC, r._file_row DESC
) = 1;

-- REF.PRODUCTS (extract + dedup)
CREATE OR REPLACE TABLE RETAIL_DB.REF.PRODUCTS AS
SELECT
  payload:sku::STRING            AS sku,
  payload:product_name::STRING   AS product_name,
  payload:category::STRING       AS category,
  payload:brand::STRING          AS brand,
  payload:price::NUMBER(10,2)    AS price,
  payload:currency::STRING       AS currency,
  _loaded_at,
  _file_name
FROM RETAIL_DB.RAW.PRODUCTS_RAW
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY payload:sku::STRING
  ORDER BY _loaded_at DESC, _file_row DESC
) = 1;

-- REF.PARTNER_ORDERS (extract + dedup)
CREATE OR REPLACE TABLE RETAIL_DB.REF.PARTNER_ORDERS AS
SELECT
  payload:partner_order_id::NUMBER            AS partner_order_id,
  TRY_TO_TIMESTAMP(payload:event_ts::STRING)  AS event_ts,
  payload:partner::STRING                     AS partner,
  payload:sku::STRING                         AS sku,
  payload:quantity::NUMBER                    AS quantity,
  payload:unit_price::NUMBER(10,2)            AS unit_price,
  payload:status::STRING                      AS status,
  _loaded_at,
  _file_name
FROM RETAIL_DB.RAW.PARTNER_ORDERS_RAW
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY payload:partner_order_id::NUMBER
  ORDER BY _loaded_at DESC, _file_row DESC
) = 1;

-- REF.HIST_SALES (typed + quality filters)
CREATE OR REPLACE TABLE RETAIL_DB.REF.HIST_SALES AS
SELECT
  ORDER_ID,
  ORDER_DATE,
  CUSTOMER_ID,
  PRODUCT_ID,
  CATEGORY,
  SUBCATEGORY,
  QUANTITY,
  COUNTRY,
  REGION,
  UNIT_PRICE,
  DISCOUNT_PCT,
  REVENUE,
  COGS
FROM RETAIL_DB.RAW.HIST_SALES_RAW
WHERE
  ORDER_ID IS NOT NULL
  AND ORDER_DATE <= CURRENT_DATE()
  AND QUANTITY > 0
  AND UNIT_PRICE >= 0
  AND DISCOUNT_PCT BETWEEN 0 AND 1
  AND REVENUE >= 0
  AND COGS BETWEEN 0 AND REVENUE;

-- REF.CUSTOMERS (aggregated)
CREATE OR REPLACE TABLE RETAIL_DB.REF.CUSTOMERS AS
SELECT
  customer_id,
  MIN(email)   AS email,
  MIN(country) AS country,
  MIN(region)  AS region,
  MIN(_loaded_at) AS first_seen_at,
  MAX(_loaded_at) AS last_seen_at
FROM RETAIL_DB.REF.SALES_TX
WHERE customer_id IS NOT NULL
GROUP BY 1;

-- Data quality checks (examples)
-- Fix: replaced `month` reference with a computed month
SELECT DATE_TRUNC('MONTH', ORDER_DATE) AS month, region, COUNT(*) c
FROM RETAIL_DB.REF.HIST_SALES
GROUP BY 1,2 HAVING COUNT(*) > 1;

SELECT order_id, COUNT(*) c
FROM RETAIL_DB.REF.SALES_TX
GROUP BY 1 HAVING COUNT(*) > 1;

SELECT sku, COUNT(*) c
FROM RETAIL_DB.REF.PRODUCTS
GROUP BY 1 HAVING COUNT(*) > 1;

SELECT partner_order_id, COUNT(*) c
FROM RETAIL_DB.REF.PARTNER_ORDERS
GROUP BY 1 HAVING COUNT(*) > 1;

SELECT COUNT(*) AS null_order_id
FROM RETAIL_DB.REF.SALES_TX
WHERE order_id IS NULL;

SELECT COUNT(*) AS null_sku
FROM RETAIL_DB.REF.PRODUCTS
WHERE sku IS NULL;

SELECT COUNT(*) AS null_partner_order_id
FROM RETAIL_DB.REF.PARTNER_ORDERS
WHERE partner_order_id IS NULL;

SELECT COUNT(*) AS row_count
FROM RETAIL_DB.RAW.HIST_SALES_RAW;
