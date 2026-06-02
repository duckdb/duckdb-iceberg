CREATE OR REPLACE TABLE default.bucket_partitioned_int_for_insert (
    id INTEGER,
    value INTEGER
)
USING iceberg
PARTITIONED BY (bucket(4, value))
