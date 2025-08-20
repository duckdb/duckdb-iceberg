CREATE or REPLACE TABLE default.empty_insert (
     col1 date,
     col2 integer,
     col3 string
)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);