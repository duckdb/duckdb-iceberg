CREATE OR REPLACE TABLE default.truncate_partitioned_decimal (
    id INTEGER,
    nested STRUCT<data: decimal(10,2)>,
    label STRING
)
USING iceberg
PARTITIONED BY (truncate(nested.data, 50))
