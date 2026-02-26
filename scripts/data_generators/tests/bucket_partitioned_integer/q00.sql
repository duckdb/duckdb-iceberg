CREATE OR REPLACE TABLE default.bucket_partitioned_integer (
    id INTEGER,
    value INTEGER,
    name STRING
)
USING iceberg
PARTITIONED BY (bucket(4, value))
