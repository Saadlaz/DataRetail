-- 06_final_tables.sql
USE ROLE SYSADMIN;
USE DATABASE RETAIL_DB;
USE WAREHOUSE WH_RETAIL;

-- View: daily sales enriched with customer region
CREATE OR REPLACE VIEW RETAIL_DB.FINAL.V_HIST_SALES_DAY AS
SELECT
  DATE_TRUNC('DAY', hs.ORDER_DATE) AS day,
  hs.ORDER_ID,
  hs.CUSTOMER_ID,
  c.REGION AS region,
  hs.PRODUCT_ID,
  hs.CATEGORY,
  hs.SUBCATEGORY,
  hs.country,
  hs.QUANTITY,
  hs.REVENUE,
  hs.COGS,
  (hs.REVENUE - hs.COGS) AS gross_margin
FROM RETAIL_DB.REF.HIST_SALES hs
LEFT JOIN RETAIL_DB.REF.CUSTOMERS c
  ON hs.CUSTOMER_ID = c.CUSTOMER_ID;

-- KPI: daily sales by region (country-level available below if needed)
CREATE OR REPLACE TABLE RETAIL_DB.FINAL.KPI_DAILY_REGION_SALES AS
SELECT
  day,
  region,
  COUNT(DISTINCT order_id) AS orders,
  COUNT(DISTINCT customer_id) AS customers,
  SUM(quantity) AS units,
  SUM(revenue) AS revenue,
  SUM(cogs) AS cogs,
  SUM(gross_margin) AS gross_margin
FROM RETAIL_DB.FINAL.V_HIST_SALES_DAY
GROUP BY 1,2;

-- KPI: daily sales by product
CREATE OR REPLACE TABLE RETAIL_DB.FINAL.KPI_DAILY_PRODUCT_SALES AS
SELECT
  day,
  PRODUCT_ID,
  COUNT(DISTINCT ORDER_ID) AS orders,
  SUM(QUANTITY) AS units,
  SUM(REVENUE) AS revenue,
  SUM(COGS) AS cogs,
  SUM(GROSS_MARGIN) AS gross_margin
FROM RETAIL_DB.FINAL.V_HIST_SALES_DAY
GROUP BY 1,2;

-- KPI: daily sales by category / subcategory
CREATE OR REPLACE TABLE RETAIL_DB.FINAL.KPI_DAILY_CATEGORY_SALES AS
SELECT
  day,
  CATEGORY,
  SUBCATEGORY,
  COUNT(DISTINCT ORDER_ID) AS orders,
  SUM(QUANTITY) AS units,
  SUM(REVENUE) AS revenue,
  SUM(COGS) AS cogs,
  SUM(GROSS_MARGIN) AS gross_margin
FROM RETAIL_DB.FINAL.V_HIST_SALES_DAY
GROUP BY 1,2,3;

-- KPI: top products overall
CREATE OR REPLACE TABLE RETAIL_DB.FINAL.KPI_TOP_PRODUCTS AS
SELECT
  PRODUCT_ID,
  COUNT(DISTINCT ORDER_ID) AS orders,
  SUM(QUANTITY) AS units,
  SUM(REVENUE) AS revenue,
  SUM(GROSS_MARGIN) AS gross_margin
FROM RETAIL_DB.FINAL.V_HIST_SALES_DAY
GROUP BY 1
ORDER BY revenue DESC;

-- Enriched daily product sales
CREATE OR REPLACE TABLE RETAIL_DB.FINAL.KPI_DAILY_PRODUCT_SALES_ENRICHED AS
SELECT
  k.day,
  k.PRODUCT_ID,
  p.product_name,
  COALESCE(p.category, h.CATEGORY) AS category,
  p.brand,
  k.orders,
  k.units,
  k.revenue,
  k.cogs,
  k.gross_margin
FROM RETAIL_DB.FINAL.KPI_DAILY_PRODUCT_SALES k
LEFT JOIN RETAIL_DB.REF.PRODUCTS p
  ON k.PRODUCT_ID = p.sku
LEFT JOIN (
  SELECT DISTINCT PRODUCT_ID, CATEGORY
  FROM RETAIL_DB.REF.HIST_SALES
) h
  ON k.PRODUCT_ID = h.PRODUCT_ID;

-- KPI: partner daily sales (from REF.PARTNER_ORDERS)
CREATE OR REPLACE TABLE RETAIL_DB.FINAL.KPI_DAILY_PARTNER_SALES AS
SELECT
  DATE_TRUNC('DAY', event_ts) AS day,
  partner,
  COUNT(DISTINCT partner_order_id) AS orders,
  SUM(quantity) AS units,
  SUM(quantity * unit_price) AS revenue
FROM RETAIL_DB.REF.PARTNER_ORDERS
WHERE event_ts IS NOT NULL
GROUP BY 1,2;

-- Enriched partner KPI
CREATE OR REPLACE TABLE RETAIL_DB.FINAL.KPI_DAILY_PARTNER_SALES_ENRICHED AS
SELECT
  DATE_TRUNC('DAY', po.event_ts) AS day,
  po.partner,
  p.category,
  p.brand,
  COUNT(DISTINCT po.partner_order_id) AS orders,
  SUM(po.quantity) AS units,
  SUM(po.quantity * po.unit_price) AS revenue
FROM RETAIL_DB.REF.PARTNER_ORDERS po
LEFT JOIN RETAIL_DB.REF.PRODUCTS p
  ON po.sku = p.sku
WHERE po.event_ts IS NOT NULL
GROUP BY 1,2,3,4;

-- Optional: country-level KPI (if you prefer country aggregation)
CREATE OR REPLACE TABLE RETAIL_DB.FINAL.KPI_DAILY_COUNTRY_SALES AS
SELECT
  day,
  country,
  COUNT(DISTINCT order_id) AS orders,
  COUNT(DISTINCT customer_id) AS customers,
  SUM(quantity) AS units,
  SUM(revenue) AS revenue,
  SUM(cogs) AS cogs,
  SUM(gross_margin) AS gross_margin
FROM RETAIL_DB.FINAL.V_HIST_SALES_DAY
GROUP BY 1,2;

-- Validation example: check for negative revenue in final KPIs
SELECT COUNT(*) AS invalid_final_revenue
FROM RETAIL_DB.FINAL.KPI_DAILY_PRODUCT_SALES
WHERE revenue < 0;

-- Quick previews
SELECT * FROM RETAIL_DB.FINAL.V_HIST_SALES_DAY LIMIT 10;
SELECT * FROM RETAIL_DB.FINAL.KPI_DAILY_PRODUCT_SALES ORDER BY day DESC LIMIT 10;
SELECT * FROM RETAIL_DB.FINAL.KPI_DAILY_CATEGORY_SALES ORDER BY day DESC LIMIT 10;
SELECT * FROM RETAIL_DB.FINAL.KPI_DAILY_PARTNER_SALES ORDER BY day DESC LIMIT 10;
