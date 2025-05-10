CREATE or REPLACE TABLE default.schema_evolve_widen_decimal (
	col DECIMAL(12, 8)
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);