CREATE or REPLACE TABLE default.month_partitioned_table (
	id int,
	ts timestamp,
	customer varchar(20),
    amount double
)
PARTITIONED BY (month(ts))
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);