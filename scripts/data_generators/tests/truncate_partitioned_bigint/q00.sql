CREATE OR REPLACE TABLE default.truncate_partitioned_bigint (
    id BIGINT,
    nested STRUCT<value: BIGINT>,
    name STRING
)
USING iceberg
PARTITIONED BY (truncate(nested.value, 100000))
