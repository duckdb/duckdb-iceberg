CREATE OR REPLACE TABLE default.bucket_partitioned_decimal_for_insert (
    id INTEGER,
    amount DECIMAL(10, 2)
)
USING iceberg
PARTITIONED BY (bucket(4, amount))
