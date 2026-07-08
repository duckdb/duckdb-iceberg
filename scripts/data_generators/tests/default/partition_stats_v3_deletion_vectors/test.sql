CREATE OR REPLACE TABLE default.partition_stats_v3_deletion_vectors (
    id INT,
    bucket STRING,
    payload STRING,
    amount BIGINT
)
USING iceberg
PARTITIONED BY (bucket)
TBLPROPERTIES (
    'format-version' = '3',
    'write.delete.mode' = 'merge-on-read',
    'write.delete.format' = 'puffin',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
);

INSERT INTO default.partition_stats_v3_deletion_vectors VALUES
    (1, 'left', 'a', 10),
    (2, 'left', 'b', 20),
    (3, 'right', 'c', 30),
    (4, 'right', 'd', 40),
    (5, NULL, 'e', 50);

CALL iceberg_catalog.system.compute_partition_stats(table => 'default.partition_stats_v3_deletion_vectors');

DELETE FROM default.partition_stats_v3_deletion_vectors
WHERE id IN (2, 3);

UPDATE default.partition_stats_v3_deletion_vectors
SET payload = CONCAT(payload, '_dv'), amount = amount + 1000
WHERE id IN (1, 4);

INSERT INTO default.partition_stats_v3_deletion_vectors VALUES
    (6, 'right', 'f', 60),
    (7, 'center', 'g', 70);

CALL iceberg_catalog.system.compute_partition_stats(table => 'default.partition_stats_v3_deletion_vectors');
