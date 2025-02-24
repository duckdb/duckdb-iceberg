CREATE or REPLACE TABLE default.lineitem_sf_01_1_delete
       TBLPROPERTIES (
        'format-version'='2',
        'write.update.mode'='merge-on-read'
       )
AS SELECT * FROM parquet_file_view;