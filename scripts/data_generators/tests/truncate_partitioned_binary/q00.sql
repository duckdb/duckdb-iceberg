CREATE OR REPLACE TABLE default.truncate_partitioned_binary (
    id INTEGER,
    nested STRUCT<data: BINARY>,
    label STRING
)
USING iceberg
PARTITIONED BY (truncate(nested.data, 2))
