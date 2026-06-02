CREATE OR REPLACE TABLE default.spark_written_upper_lower_hugeint_bounds (
    id       INTEGER,
    val38_0  DECIMAL(38, 0)
)
USING ICEBERG
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.target-file-size-bytes' = '2132'
);
