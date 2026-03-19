CREATE OR REPLACE TABLE default.date_identity_partitioned_table (
    dt     date,
    number integer,
    letter string
)
USING iceberg
PARTITIONED BY (dt);
