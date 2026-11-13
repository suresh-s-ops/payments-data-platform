-- Batch extracts from Hadoop are pushed to a Snowflake internal stage
-- (PUT from the edge node) and loaded with COPY. COPY tracks loaded files,
-- so re-running the same load never duplicates rows.
CREATE STAGE IF NOT EXISTS raw.payments.settlements_stage
  FILE_FORMAT = (TYPE = PARQUET);

COPY INTO raw.payments.settlements_raw
FROM @raw.payments.settlements_stage
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Stream captures only new raw rows; a task upserts them into silver.
-- A payment moves through a lifecycle: authorized -> captured -> (refunded).
-- MERGE keeps one row per attempt holding its LATEST state, so reports never
-- double-count an attempt that was first authorized and then captured.
CREATE STREAM IF NOT EXISTS raw.payments.payment_attempts_stream
  ON TABLE raw.payments.payment_attempts_raw;

CREATE OR REPLACE TASK silver.payments.merge_payment_attempts
  WAREHOUSE = transform_wh
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('raw.payments.payment_attempts_stream')
AS
MERGE INTO silver.payments.payment_attempts t
USING (
    SELECT payload:attempt_id::string     AS attempt_id,
           payload:order_id::string       AS order_id,
           payload:customer_id::string    AS customer_id,
           payload:status::string         AS status,
           payload:amount::number(18,2)   AS amount,
           payload:currency::string       AS currency,
           payload:processor::string      AS processor,
           payload:event_ts::timestamp_ntz AS event_ts
    FROM (SELECT PARSE_JSON(payload) AS payload FROM raw.payments.payment_attempts_stream)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attempt_id ORDER BY event_ts DESC) = 1
) s
ON t.attempt_id = s.attempt_id
WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET
     status = s.status, amount = s.amount, event_ts = s.event_ts
WHEN NOT MATCHED THEN INSERT
     (attempt_id, order_id, customer_id, status, amount, currency, processor, event_ts)
     VALUES (s.attempt_id, s.order_id, s.customer_id, s.status, s.amount,
             s.currency, s.processor, s.event_ts);

-- PII governance: only approved roles see raw customer identifiers.
-- Analysts can still count and join customers (hash is consistent) without
-- seeing who they are: least-privilege access for regulated payments data.
CREATE MASKING POLICY IF NOT EXISTS governance.pii_mask AS (val STRING)
RETURNS STRING ->
  CASE WHEN IS_ROLE_IN_SESSION('PAYMENTS_PII_READER') THEN val
       ELSE SHA2(val) END;

ALTER TABLE silver.payments.payment_attempts
  MODIFY COLUMN customer_id SET MASKING POLICY governance.pii_mask;
