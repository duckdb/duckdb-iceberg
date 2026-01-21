INSERT INTO default.spark_written_upper_lower_bounds_all_null
SELECT
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
FROM (SELECT explode(sequence(1, 1000)) AS dummy);
