CREATE OR REPLACE TABLE default.bucket_partitioned_timestamp (
    id INTEGER,
    ts_val TIMESTAMP,
    label STRING
)
USING iceberg
PARTITIONED BY (bucket(4, ts_val))
