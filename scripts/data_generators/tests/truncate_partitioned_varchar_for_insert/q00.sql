CREATE OR REPLACE TABLE default.truncate_partitioned_varchar_for_insert (
    id INTEGER,
    value STRING
)
USING iceberg
PARTITIONED BY (truncate(value, 3))
