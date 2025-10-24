CREATE OR REPLACE TABLE default.table_sort_order
USING iceberg
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
)
as select * from parquet_file_view;
