CREATE OR REPLACE TABLE default.lineitem_partitioned_l_shipmode_deletes
USING iceberg
PARTITIONED BY (l_shipmode)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
)
as select * from parquet_file_view;
