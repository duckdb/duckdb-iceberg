CREATE or REPLACE TABLE default.simple_v3_table
TBLPROPERTIES (
	'format-version' = '3',
	'write.delete.mode' = 'merge-on-read',
	'write.delete.format' = 'puffin',
	'write.update.mode' = 'merge-on-read'
)
AS SELECT * FROM parquet_file_view;
