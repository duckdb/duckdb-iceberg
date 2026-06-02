CREATE OR REPLACE TABLE default.truncate_partitioned_int_for_insert (
    id INTEGER,
    value INTEGER
)
USING iceberg
PARTITIONED BY (truncate(value, 10))
