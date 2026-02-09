CREATE or REPLACE TABLE default.month_partitioned_date (
	id int,
	dt date,
	customer varchar(20),
    amount double
)
PARTITIONED BY (month(dt))
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);
