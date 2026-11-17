CREATE OR REPLACE TABLE default.row_lineage_upgraded_deleted_files (id BIGINT, p INT)
PARTITIONED BY (p)
TBLPROPERTIES (
    'format-version'='2'
);

INSERT INTO default.row_lineage_upgraded_deleted_files
SELECT id, CAST(id % 10 AS INT) FROM range(0, 1000);

-- Removes whole files, which stay in the manifest as DELETED entries
DELETE FROM default.row_lineage_upgraded_deleted_files WHERE p IN (0, 2, 4, 6, 8);

ALTER TABLE default.row_lineage_upgraded_deleted_files SET TBLPROPERTIES ('format-version'='3');

-- The first V3 snapshot gives the V2 manifests a first_row_id, but not their files
INSERT INTO default.row_lineage_upgraded_deleted_files
SELECT id, CAST(id % 10 AS INT) FROM range(1000, 1100);

CREATE OR REPLACE TABLE default.row_lineage_upgraded_deleted_files_expected AS
SELECT id, _row_id AS row_id FROM default.row_lineage_upgraded_deleted_files;
