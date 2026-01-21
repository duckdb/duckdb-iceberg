INSERT INTO default.spark_written_upper_lower_bounds_single_value
SELECT
    42,
    9223372036854775807,
    'constant_string',
    true,
    3.14159,
    2.718281828459045,
    123.456,
    DATE '2024-06-15',
    TIMESTAMP '2024-06-15 12:30:45.123456',
    X'FFFFFFFF'
FROM (SELECT explode(sequence(1, 1000)) AS dummy);
