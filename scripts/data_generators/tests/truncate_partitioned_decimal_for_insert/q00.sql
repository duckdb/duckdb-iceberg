CREATE OR REPLACE TABLE default.truncate_partitioned_decimal_for_insert (
    id INTEGER,
    amount DECIMAL(10, 2)
)
USING iceberg
PARTITIONED BY (truncate(amount, 100))
