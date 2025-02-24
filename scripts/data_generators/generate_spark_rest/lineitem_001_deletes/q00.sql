CREATE or REPLACE TABLE default.lineitem_001_deletes
       TBLPROPERTIES (
        'format-version'='2',
        'write.update.mode'='merge-on-read'
       )
AS SELECT * FROM parquet_file_view;