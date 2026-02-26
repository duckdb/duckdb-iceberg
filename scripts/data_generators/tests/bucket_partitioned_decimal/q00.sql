CREATE OR REPLACE TABLE default.bucket_partitioned_decimal (
    id INTEGER,
    amount DECIMAL(10, 2),
    label STRING
)
USING iceberg
PARTITIONED BY (bucket(4, amount))
