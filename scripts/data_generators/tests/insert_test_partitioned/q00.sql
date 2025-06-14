CREATE or REPLACE TABLE default.insert_test_partitioned (
	col1 date,
	col2 integer,
	col3 string
)
USING iceberg
PARTITIONED BY (col1)
