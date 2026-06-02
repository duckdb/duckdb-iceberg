CREATE OR REPLACE TABLE default.bucket_partitioned_timestamp_for_insert (
    id INTEGER,
    value TIMESTAMP
)
USING iceberg
PARTITIONED BY (bucket(4, value))
