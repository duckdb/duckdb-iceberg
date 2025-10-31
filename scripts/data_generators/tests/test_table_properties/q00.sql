CREATE or REPLACE TABLE default.test_table_properties (a int)
TBLPROPERTIES (
	'format-version' = '2',
	'write.delete.mode' = 'merge-on-read'
);
