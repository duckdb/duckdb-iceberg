CREATE or REPLACE TABLE default.partitioned_deletion_vectors (
    seq integer,
    col integer
)
USING iceberg
PARTITIONED BY (seq)
TBLPROPERTIES (
    'format-version' = '3',
    'write.delete.mode' = 'merge-on-read',
    'write.delete.format' = 'puffin'
);
INSERT INTO default.partitioned_deletion_vectors SELECT 1 as seq, id as col FROM range(0, 1000);
INSERT INTO default.partitioned_deletion_vectors SELECT 2 as seq, id as col FROM range(0, 1000);
INSERT INTO default.partitioned_deletion_vectors SELECT 3 as seq, id as col FROM range(0, 1000)
