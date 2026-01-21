CREATE OR REPLACE TABLE default.spark_written_upper_lower_bounds (
    int_type INTEGER,
    bigint_type BIGINT,
    varchar_type VARCHAR(100),
    bool_type BOOLEAN,
    float_type FLOAT,
    double_type DOUBLE,
    decimal_type_18_3 DECIMAL(18, 3),
    date_type DATE,
--     time_type TIME,
    timestamp_type TIMESTAMP,
--     timestamp_tz_type TIMESTAMPTZ,
--     uuid_type UUID,
    binary_type BINARY
)
USING ICEBERG
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.target-file-size-bytes' = '2132'
);