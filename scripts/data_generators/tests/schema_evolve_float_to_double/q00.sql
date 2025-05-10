CREATE or REPLACE TABLE default.schema_evolve_float_to_double (
	col float
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);