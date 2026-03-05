CREATE OR REPLACE TABLE default.bucket_partitioned_date (
    id INTEGER,
    event_date DATE,
    event_name STRING
)
USING iceberg
PARTITIONED BY (bucket(4, event_date))
