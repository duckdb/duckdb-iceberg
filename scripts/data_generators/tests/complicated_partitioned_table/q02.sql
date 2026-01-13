ALTER TABLE default.complicated_partitioned_table
REPLACE PARTITION FIELD year(ts)
WITH month(ts)