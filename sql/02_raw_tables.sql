-- 02_raw_tables.sql
USE ROLE SYSADMIN;
USE DATABASE RETAIL_DB;
USE SCHEMA RAW;
USE WAREHOUSE WH_RETAIL;

-- RAW tables
CREATE OR REPLACE TABLE SALES_TX_RAW (
  order_id INT,
  order_ts TIMESTAMP,
  customer_id INT,
  email STRING,
  country STRING,
  amount NUMBER(10,2),
  currency STRING,
  payment_type STRING,
  _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _file_name STRING,
  _file_row NUMBER
);

COPY INTO RETAIL_DB.RAW.SALES_TX_RAW
(order_id, order_ts, customer_id, email, country, amount, currency, payment_type, _file_name, _file_row)
FROM (
  SELECT
    $1, $2, $3, $4, $5, $6, $7, $8,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER
  FROM @RETAIL_DB.RAW.STAGE_RETAIL_S3/sales_transactions.csv
)
FILE_FORMAT = RETAIL_DB.RAW.FF_CSV;

CREATE OR REPLACE TABLE PRODUCTS_RAW (
  payload VARIANT,
  _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _file_name STRING,
  _file_row NUMBER
);

COPY INTO RETAIL_DB.RAW.PRODUCTS_RAW
(payload, _file_name, _file_row)
FROM (
  SELECT
    $1,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER
  FROM @RETAIL_DB.RAW.STAGE_RETAIL_S3/products.json
)
FILE_FORMAT = RETAIL_DB.RAW.FF_NDJSON;

CREATE OR REPLACE TABLE PARTNER_ORDERS_RAW (
  payload VARIANT,
  _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _file_name STRING,
  _file_row NUMBER
);

COPY INTO RETAIL_DB.RAW.PARTNER_ORDERS_RAW
(payload, _file_name, _file_row)
FROM (
  SELECT
    $1,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER
  FROM @RETAIL_DB.RAW.STAGE_RETAIL_S3/partner_orders.json
)
FILE_FORMAT = RETAIL_DB.RAW.FF_NDJSON;

CREATE OR REPLACE TABLE RETAIL_DB.RAW.HIST_SALES_RAW (
    order_id INT,
    order_date DATE,
    customer_id STRING,
    product_id STRING,
    category STRING,
    subcategory STRING,
    quantity INT,
    unit_price NUMBER(12,2),
    discount_pct NUMBER(10,4),
    revenue NUMBER(12,2),
    cogs NUMBER(12,2),
    profit NUMBER(12,2),
    city STRING,
    region STRING,
    country STRING,
    channel STRING,
    currency STRING
);

COPY INTO RETAIL_DB.RAW.HIST_SALES_RAW
FROM (
    SELECT
        SPLIT_PART(TRIM($1, '"'), ',', 1)::INT,
        SPLIT_PART(TRIM($1, '"'), ',', 2)::DATE,
        SPLIT_PART(TRIM($1, '"'), ',', 3)::STRING,
        SPLIT_PART(TRIM($1, '"'), ',', 4)::STRING,
        SPLIT_PART(TRIM($1, '"'), ',', 5)::STRING,
        SPLIT_PART(TRIM($1, '"'), ',', 6)::STRING,
        SPLIT_PART(TRIM($1, '"'), ',', 7)::INT,
        SPLIT_PART(TRIM($1, '"'), ',', 8)::NUMBER(12,2),
        SPLIT_PART(TRIM($1, '"'), ',', 9)::NUMBER(10,4),
        SPLIT_PART(TRIM($1, '"'), ',', 10)::NUMBER(12,2),
        SPLIT_PART(TRIM($1, '"'), ',', 11)::NUMBER(12,2),
        SPLIT_PART(TRIM($1, '"'), ',', 12)::NUMBER(12,2),
        SPLIT_PART(TRIM($1, '"'), ',', 13)::STRING,
        SPLIT_PART(TRIM($1, '"'), ',', 14)::STRING,
        SPLIT_PART(TRIM($1, '"'), ',', 15)::STRING,
        SPLIT_PART(TRIM($1, '"'), ',', 16)::STRING,
        SPLIT_PART(TRIM($1, '"'), ',', 17)::STRING
    FROM @RETAIL_DB.RAW.STAGE_RETAIL_S3/historical_sales_fixed.csv
)
FILE_FORMAT = RETAIL_DB.RAW.FF_CSV_INFERENCE;

  -- If your files are in S3 and you created the external stage, use the S3 stage paths instead, e.g.:
  -- COPY INTO RETAIL_DB.RAW.PRODUCTS_RAW (payload, _file_name, _file_row)
  -- FROM (
  --   SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
  --   FROM @RETAIL_DB.RAW.STAGE_RETAIL_S3/products.json
  -- ) FILE_FORMAT = RETAIL_DB.RAW.FF_NDJSON;

-- Quick counts
SELECT COUNT(*) FROM SALES_TX_RAW;
SELECT COUNT(*) FROM PRODUCTS_RAW;
SELECT COUNT(*) FROM PARTNER_ORDERS_RAW;
SELECT COUNT(*) FROM HIST_SALES_RAW;
LIST @RETAIL_DB.RAW.STAGE_RETAIL;
