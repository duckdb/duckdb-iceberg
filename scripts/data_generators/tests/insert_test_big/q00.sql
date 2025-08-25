CREATE or REPLACE TABLE default.insert_test_big (
	col1 INTEGER,
	col2 VARCHAR(17)
)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);
