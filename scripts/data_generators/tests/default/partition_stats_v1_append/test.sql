CREATE OR REPLACE TABLE default.partition_stats_v1_append (
    id INT,
    region STRING,
    category STRING,
    amount BIGINT
)
USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES (
    'format-version' = '1'
);

INSERT INTO default.partition_stats_v1_append VALUES
    (1, 'eu', 'seed', 10),
    (2, 'us', 'seed', 20),
    (3, 'eu', 'seed', 30),
    (4, NULL, 'seed', 40);

CALL iceberg_catalog.system.compute_partition_stats(table => 'default.partition_stats_v1_append');

INSERT INTO default.partition_stats_v1_append VALUES
    (5, 'apac', 'append', 50),
    (6, 'us', 'append', 60),
    (7, NULL, 'append', 70);

CALL iceberg_catalog.system.compute_partition_stats(table => 'default.partition_stats_v1_append');
