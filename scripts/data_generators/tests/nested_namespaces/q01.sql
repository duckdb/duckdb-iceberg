CREATE OR REPLACE TABLE demo.level1.level2.level3.nested_namespaces (
    col1 STRING,
    col2 INTEGER,
    col3 STRING
)
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read'
);