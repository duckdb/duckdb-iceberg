CREATE OR REPLACE TABLE default.year_timestamptz (
    partition_col TIMESTAMP,
    user_id BIGINT,
    event_type STRING
)
USING iceberg
PARTITIONED BY (year(partition_col))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read'
);
