CREATE or REPLACE TABLE default.complicated_partitioned_table (
	id int,
	ts timestamp,
	customer varchar(20),
    amount long
)
PARTITIONED BY (year(ts))
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);