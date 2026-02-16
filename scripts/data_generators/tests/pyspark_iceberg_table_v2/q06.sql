ALTER TABLE default.pyspark_iceberg_table_v2
	ADD COLUMN schema_evol_added_col_1 INT;
UPDATE default.pyspark_iceberg_table_v2
	SET schema_evol_added_col_1 = 42;