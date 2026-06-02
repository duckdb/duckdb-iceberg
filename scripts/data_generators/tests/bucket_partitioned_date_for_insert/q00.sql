CREATE OR REPLACE TABLE default.bucket_partitioned_date_for_insert (
    id INTEGER,
    value DATE
)
USING iceberg
PARTITIONED BY (bucket(4, value))
