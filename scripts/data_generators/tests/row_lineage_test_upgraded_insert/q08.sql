ALTER TABLE default.row_lineage_test_upgraded_insert
SET TBLPROPERTIES (
	'format-version'='3',
    'write.delete.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
);