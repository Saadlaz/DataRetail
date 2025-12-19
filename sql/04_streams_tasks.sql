-- 04_streams_tasks.sql
USE ROLE SYSADMIN;
USE DATABASE RETAIL_DB;
USE WAREHOUSE WH_RETAIL;

-- REF.PARTNER_ORDERS_CUR (current/upsert table)
CREATE OR REPLACE TABLE RETAIL_DB.REF.PARTNER_ORDERS_CUR (
  partner_order_id NUMBER,
  event_ts         TIMESTAMP,
  partner          STRING,
  sku              STRING,
  quantity         NUMBER,
  unit_price       NUMBER(10,2),
  status           STRING,
  _loaded_at       TIMESTAMP,
  _file_name       STRING,
  _file_row        NUMBER
);

-- Stream on raw ingested table (append-only stream example)
CREATE OR REPLACE STREAM RETAIL_DB.RAW.STR_PARTNER_ORDERS_RAW
ON TABLE RETAIL_DB.RAW.PARTNER_ORDERS_RAW
APPEND_ONLY = TRUE;

-- Stored procedure to merge stream changes into current table and refresh incremental KPI partitions
CREATE OR REPLACE PROCEDURE RETAIL_DB.SEC.SP_PARTNER_INCREMENTAL_REFRESH()
RETURNS STRING
LANGUAGE SQL
AS

DECLARE
  v_days NUMBER;
  v_rows NUMBER;
BEGIN

  IF (NOT SYSTEM$STREAM_HAS_DATA('RETAIL_DB.RAW.STR_PARTNER_ORDERS_RAW')) THEN
    RETURN 'NOOP - stream empty';
  END IF;

  CREATE OR REPLACE TEMP TABLE TMP_STREAM AS
  SELECT
    payload:partner_order_id::NUMBER            AS partner_order_id,
    TRY_TO_TIMESTAMP(payload:event_ts::STRING)  AS event_ts,
    payload:partner::STRING                     AS partner,
    payload:sku::STRING                         AS sku,
    TRY_TO_NUMBER(payload:quantity)             AS quantity,
    TRY_TO_NUMBER(payload:unit_price)           AS unit_price,
    payload:status::STRING                      AS status,
    _loaded_at,
    _file_name,
    _file_row
  FROM RETAIL_DB.RAW.STR_PARTNER_ORDERS_RAW
  WHERE payload:partner_order_id IS NOT NULL;

  v_rows := (SELECT COUNT(*) FROM TMP_STREAM);
  IF (v_rows = 0) THEN
    RETURN 'NOOP - stream rows = 0';
  END IF;

  MERGE INTO RETAIL_DB.REF.PARTNER_ORDERS_CUR t
  USING TMP_STREAM s
  ON t.partner_order_id = s.partner_order_id
  WHEN MATCHED AND (COALESCE(t._loaded_at, '1970-01-01') < s._loaded_at) THEN UPDATE SET
    event_ts   = s.event_ts,
    partner    = s.partner,
    sku        = s.sku,
    quantity   = s.quantity,
    unit_price = s.unit_price,
    status     = s.status,
    _loaded_at = s._loaded_at,
    _file_name = s._file_name,
    _file_row  = s._file_row
  WHEN NOT MATCHED THEN INSERT (
    partner_order_id, event_ts, partner, sku, quantity, unit_price, status,
    _loaded_at, _file_name, _file_row
  ) VALUES (
    s.partner_order_id, s.event_ts, s.partner, s.sku, s.quantity, s.unit_price, s.status,
    s._loaded_at, s._file_name, s._file_row
  );

  CREATE OR REPLACE TEMP TABLE TMP_AFFECTED_DAYS AS
  SELECT DISTINCT DATE_TRUNC('DAY', event_ts)::DATE AS day
  FROM TMP_STREAM
  WHERE event_ts IS NOT NULL;

  v_days := (SELECT COUNT(*) FROM TMP_AFFECTED_DAYS);

  CREATE TABLE IF NOT EXISTS RETAIL_DB.FINAL.KPI_DAILY_PARTNER_SALES_INC (
    day     DATE,
    partner STRING,
    orders  NUMBER,
    units   NUMBER,
    revenue NUMBER(38,2)
  );

  DELETE FROM RETAIL_DB.FINAL.KPI_DAILY_PARTNER_SALES_INC k
  USING TMP_AFFECTED_DAYS d
  WHERE k.day = d.day;

  INSERT INTO RETAIL_DB.FINAL.KPI_DAILY_PARTNER_SALES_INC (day, partner, orders, units, revenue)
  SELECT
    DATE_TRUNC('DAY', event_ts)::DATE AS day,
    partner,
    COUNT(DISTINCT partner_order_id) AS orders,
    SUM(quantity) AS units,
    SUM(quantity * unit_price) AS revenue
  FROM RETAIL_DB.REF.PARTNER_ORDERS_CUR
  WHERE event_ts IS NOT NULL
    AND DATE_TRUNC('DAY', event_ts)::DATE IN (SELECT day FROM TMP_AFFECTED_DAYS)
  GROUP BY 1,2;

  RETURN 'OK - stream_rows=' || v_rows || ' affected_days=' || v_days;

END;
;

-- Task to call the proc periodically (adjust schedule as needed)
CREATE OR REPLACE TASK RETAIL_DB.SEC.TSK_PARTNER_INCREMENTAL_REFRESH
  WAREHOUSE = WH_RETAIL
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DB.RAW.STR_PARTNER_ORDERS_RAW')
AS
  CALL RETAIL_DB.SEC.SP_PARTNER_INCREMENTAL_REFRESH();

ALTER TASK RETAIL_DB.SEC.TSK_PARTNER_INCREMENTAL_REFRESH RESUME;

SHOW TASKS IN SCHEMA RETAIL_DB.SEC;
