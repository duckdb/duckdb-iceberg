CREATE OR REPLACE TABLE default.partition_stats_v2_mor_deletes_updates (
    id INT,
    region STRING,
    status STRING,
    amount BIGINT
)
USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
);

INSERT INTO default.partition_stats_v2_mor_deletes_updates VALUES
    (1, 'eu', 'new', 10),
    (2, 'eu', 'new', 20),
    (3, 'us', 'new', 30),
    (4, 'us', 'new', 40),
    (5, NULL, 'new', 50);

CALL iceberg_catalog.system.compute_partition_stats(table => 'default.partition_stats_v2_mor_deletes_updates');

UPDATE default.partition_stats_v2_mor_deletes_updates
SET amount = amount + 100, status = 'updated'
WHERE id IN (2, 4);

DELETE FROM default.partition_stats_v2_mor_deletes_updates
WHERE id IN (1, 5);

INSERT INTO default.partition_stats_v2_mor_deletes_updates VALUES
    (6, 'apac', 'late', 60),
    (7, 'eu', 'late', 70);

CALL iceberg_catalog.system.compute_partition_stats(table => 'default.partition_stats_v2_mor_deletes_updates');
