CREATE OR REPLACE TABLE default.timestamp_identity_partitioned_table (
    dt     TIMESTAMP,
    number integer,
    letter string
)
USING iceberg
PARTITIONED BY (dt);
