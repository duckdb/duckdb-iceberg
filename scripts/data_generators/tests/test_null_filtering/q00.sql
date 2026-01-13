CREATE or REPLACE TABLE default.test_null_filtering (a int, b VARCHAR(20))
TBLPROPERTIES (
	'format-version' = '2',
	'write.delete.mode' = 'merge-on-read',
	'write.update.mode' = 'merge-on-read'
);
