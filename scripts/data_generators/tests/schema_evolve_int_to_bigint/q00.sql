CREATE or REPLACE TABLE default.schema_evolve_int_to_bigint (
	col integer
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);