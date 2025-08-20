CREATE or REPLACE TABLE default.deletion_vectors
TBLPROPERTIES (
	'format-version' = '3',
	'write.delete.mode' = 'merge-on-read',
	'write.delete.format' = 'puffin',
	'write.update.mode' = 'merge-on-read'
)
AS SELECT * FROM parquet_file_view;
