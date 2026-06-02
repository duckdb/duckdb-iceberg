CREATE OR REPLACE TABLE default.bucket_partitioned_bigint_for_insert (
    id INTEGER,
    value BIGINT
)
USING iceberg
PARTITIONED BY (bucket(4, value))
