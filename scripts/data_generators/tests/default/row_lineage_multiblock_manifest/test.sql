CREATE OR REPLACE TABLE default.row_lineage_multiblock_manifest (id BIGINT, a STRING, b STRING, c STRING)
TBLPROPERTIES (
    'format-version'='3',
    'write.metadata.metrics.default'='full'
);

-- Long strings with full metrics make large manifest entries, so the manifest has several Avro blocks
INSERT INTO default.row_lineage_multiblock_manifest
SELECT /*+ REPARTITION(200) */ id, concat(repeat('a', 500), id), concat(repeat('b', 500), id), concat(repeat('c', 500), id)
FROM range(0, 2000);

INSERT INTO default.row_lineage_multiblock_manifest
SELECT /*+ REPARTITION(20) */ id, concat(repeat('a', 500), id), concat(repeat('b', 500), id), concat(repeat('c', 500), id)
FROM range(2000, 2200);

CREATE OR REPLACE TABLE default.row_lineage_multiblock_manifest_expected AS
SELECT id, _row_id AS row_id FROM default.row_lineage_multiblock_manifest;
