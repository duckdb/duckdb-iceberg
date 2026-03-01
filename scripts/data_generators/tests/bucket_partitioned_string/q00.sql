CREATE OR REPLACE TABLE default.bucket_partitioned_string (
    id INTEGER,
    category STRING,
    description STRING
)
USING iceberg
PARTITIONED BY (bucket(4, category))
