CREATE OR REPLACE TABLE default.partition_stats_partition_evolution (
    id INT,
    ts TIMESTAMP,
    customer STRING,
    amount BIGINT
)
USING iceberg
PARTITIONED BY (year(ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
);

INSERT INTO default.partition_stats_partition_evolution VALUES
    (1, TIMESTAMP '2020-01-01 00:00:00', 'c1', 10),
    (2, TIMESTAMP '2020-06-01 00:00:00', 'c2', 20),
    (3, TIMESTAMP '2021-01-15 00:00:00', 'c3', 30),
    (4, NULL, 'c4', 40);

CALL iceberg_catalog.system.compute_partition_stats(table => 'default.partition_stats_partition_evolution');

ALTER TABLE default.partition_stats_partition_evolution
REPLACE PARTITION FIELD year(ts)
WITH month(ts);

INSERT INTO default.partition_stats_partition_evolution VALUES
    (5, TIMESTAMP '2020-02-03 00:00:00', 'c5', 50),
    (6, TIMESTAMP '2021-02-04 00:00:00', 'c6', 60),
    (7, NULL, 'c7', 70);

UPDATE default.partition_stats_partition_evolution
SET amount = amount + 500
WHERE id IN (3, 5);

DELETE FROM default.partition_stats_partition_evolution
WHERE id IN (2, 4);

CALL iceberg_catalog.system.compute_partition_stats(table => 'default.partition_stats_partition_evolution');
