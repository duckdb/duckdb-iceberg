CREATE OR REPLACE TABLE default.spark_rewrite_entry_snapshot_ids (
    id INT,
    category STRING
)
USING ICEBERG
PARTITIONED BY (category)
TBLPROPERTIES (
    'format-version' = '2'
);

INSERT INTO default.spark_rewrite_entry_snapshot_ids VALUES (1, 'a'), (2, 'b');

INSERT INTO default.spark_rewrite_entry_snapshot_ids VALUES (3, 'a');
