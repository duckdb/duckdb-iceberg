CREATE OR REPLACE TABLE default.truncate_partitioned_binary_for_insert (
    id INTEGER,
    value BINARY
)
USING iceberg
PARTITIONED BY (truncate(value, 2))
