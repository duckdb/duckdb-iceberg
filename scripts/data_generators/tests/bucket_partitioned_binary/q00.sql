CREATE OR REPLACE TABLE default.bucket_partitioned_binary (
    id INTEGER,
    data BINARY,
    label STRING
)
USING iceberg
PARTITIONED BY (bucket(4, data))
