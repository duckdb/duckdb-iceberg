CREATE OR REPLACE TABLE default.spark_written_upper_lower_bounds (
    a INTEGER,
    b INTEGER
)
USING ICEBERG
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.target-file-size-bytes' = '2132'
);