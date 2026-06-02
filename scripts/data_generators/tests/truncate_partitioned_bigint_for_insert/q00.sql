CREATE OR REPLACE TABLE default.truncate_partitioned_bigint_for_insert (
    id INTEGER,
    value BIGINT
)
USING iceberg
PARTITIONED BY (truncate(value, 10))
