CREATE OR REPLACE TABLE default.bucket_partitioned_varchar_for_insert (
    id INTEGER,
    value STRING
)
USING iceberg
PARTITIONED BY (bucket(8, value))
