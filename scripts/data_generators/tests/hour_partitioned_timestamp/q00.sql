CREATE or REPLACE TABLE default.hour_partitioned_timestamp (
	id int,
	dt TIMESTAMP,
	customer varchar(20),
    amount double
)
PARTITIONED BY (hours(dt))
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);
