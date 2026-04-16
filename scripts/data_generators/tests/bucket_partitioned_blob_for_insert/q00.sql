CREATE OR REPLACE TABLE default.bucket_partitioned_blob_for_insert (
    id INTEGER,
    value BINARY
)
USING iceberg
PARTITIONED BY (bucket(4, value))
