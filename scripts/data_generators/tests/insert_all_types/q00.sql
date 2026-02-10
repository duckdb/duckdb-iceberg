CREATE OR REPLACE TABLE default.insert_all_types (
	int_col INT,
	long_col BIGINT,
	float_col FLOAT,
	double_col DOUBLE,
	decimal_col DECIMAL(15, 5),
	date_col DATE
) USING ICEBERG;
