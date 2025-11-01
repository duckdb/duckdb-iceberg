CREATE or REPLACE TABLE default.day_partitioned_table (
	id int,
	ts timestamp,
	customer varchar(20),
    amount double
)
PARTITIONED BY (day(ts))
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);