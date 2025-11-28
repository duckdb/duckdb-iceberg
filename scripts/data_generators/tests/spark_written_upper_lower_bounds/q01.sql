INSERT INTO default.spark_written_upper_lower_bounds VALUES
-- Lower bounds
(
    -2147483648,                         -- int_type (Integer min)
    -9223372036854775808,               -- bigint_type (Long min)
    '',                                  -- varchar_type (empty string as lower bound)
    false,                               -- bool_type
    -3.4028235E38,                       -- float_type (Float min)
    -1.7976931348623157E308,            -- double_type (Double min)
    -9999999999999.999,                 -- decimal(18,3) lower bound
    0.0,                                 -- decimal_type_1_12 (placeholder: adjust to your real definition)
    DATE '0001-01-01',                   -- date_type (Spark's min date)
--     TIME '00:00:00',                     -- time_type
    TIMESTAMP '0001-01-01 00:00:00',     -- timestamp_type
--     TIMESTAMPTZ '0001-01-01 00:00:00+00',-- timestamp_tz_type
--     UUID '00000000-0000-0000-0000-000000000000', -- uuid_type
    X''                                   -- binary_type (empty binary)
),
-- Upper bounds
(
    2147483647,                          -- int_type (Integer max)
    9223372036854775807,                 -- bigint_type (Long max)
    RPAD('Z', 100, 'Z'),                 -- varchar_type (max-length string)
    true,                                -- bool_type
    3.4028235E38,                        -- float_type (Float max)
    1.7976931348623157E308,             -- double_type (Double max)
    9999999999999.999,                  -- decimal(18,3) upper bound
    9.9,                                 -- decimal_type_1_12 (placeholder)
    DATE '9999-12-31',                   -- date_type
--     TIME '23:59:59.999999',              -- time_type (microsecond max)
    TIMESTAMP '9999-12-31 23:59:59.999999', -- timestamp_type
--     TIMESTAMPTZ '9999-12-31 23:59:59.999999+00', -- timestamp_tz_type
--     UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff', -- uuid_type
    X'FFFFFFFF'                           -- binary_type (example max-ish binary)
);
