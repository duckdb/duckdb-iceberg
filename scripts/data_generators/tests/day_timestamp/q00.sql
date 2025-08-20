CREATE OR REPLACE TABLE default.day_timestamp (
    partition_col TIMESTAMP_NTZ,
    user_id BIGINT,
    event_type STRING
)
USING iceberg
PARTITIONED BY (day(partition_col))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read'
);
