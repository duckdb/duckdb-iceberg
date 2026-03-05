CREATE or REPLACE TABLE default.variant_column
TBLPROPERTIES (
	'format-version'='3',
	'write.update.mode'='copy-on-write'
)
AS SELECT col::VARIANT FROM parquet_file_view t(col);