CREATE OR REPLACE TABLE default.bucket_partitioned_time_for_insert (
    id INTEGER,
    value TIME
)
USING iceberg
PARTITIONED BY (bucket(4, value))
