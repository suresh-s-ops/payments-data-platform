-- Load the day's settlement extract (staged from Hadoop) into the raw layer.
-- COPY tracks which files were already loaded, so reruns never duplicate rows.
COPY INTO raw.payments.settlements_raw
FROM @raw.payments.settlements_stage/dt={{ ds }}/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
