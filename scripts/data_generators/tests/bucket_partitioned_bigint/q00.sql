CREATE OR REPLACE TABLE default.bucket_partitioned_bigint (
    id BIGINT,
    value BIGINT,
    name STRING
)
USING iceberg
PARTITIONED BY (bucket(8, value))
