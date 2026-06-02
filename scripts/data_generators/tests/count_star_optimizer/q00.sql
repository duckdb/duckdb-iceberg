CREATE OR REPLACE TABLE default.count_star_optimizer (
    id INT,
    name STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.data.partition-columns' = false,
    'write.parquet.write-partition-values' = false
);