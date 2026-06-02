CREATE OR REPLACE TABLE default.spark_written_upper_lower_decimal_bounds (
    id       INTEGER,
    val10_2  DECIMAL(10, 2),
    val18_4  DECIMAL(18, 4),
    val5_2   DECIMAL(5, 2),
    val18_0  DECIMAL(18, 0),
    val38_0  DECIMAL(38, 0),
    val7_1   DECIMAL(7, 1),
    val12_2  DECIMAL(12, 2)
)
USING ICEBERG
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.target-file-size-bytes' = '2132'
);
