CREATE OR REPLACE TABLE default.row_lineage_test_upgraded (
  id INT,
  data STRING
)
TBLPROPERTIES (
    'format-version'='2',
    'write.delete.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
);
