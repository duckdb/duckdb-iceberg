CREATE OR REPLACE TABLE default.truncate_partitioned_varchar (
    id INTEGER,
    nested STRUCT<data: VARCHAR(16)>,
    label STRING
)
USING iceberg
PARTITIONED BY (truncate(nested.data, 2))
