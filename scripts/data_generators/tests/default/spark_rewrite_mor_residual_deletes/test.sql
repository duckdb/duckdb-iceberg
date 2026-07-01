CREATE OR REPLACE TABLE default.spark_rewrite_mor_residual_deletes (
    id INT,
    category STRING,
    payload STRING
)
USING ICEBERG
PARTITIONED BY (category)
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
);

INSERT INTO default.spark_rewrite_mor_residual_deletes VALUES
    (1, 'compact', 'c1'),
    (2, 'compact', 'c2');

INSERT INTO default.spark_rewrite_mor_residual_deletes VALUES
    (3, 'compact', 'c3'),
    (4, 'compact', 'c4');

INSERT INTO default.spark_rewrite_mor_residual_deletes VALUES
    (5, 'compact', 'c5'),
    (6, 'compact', 'c6');

INSERT INTO default.spark_rewrite_mor_residual_deletes VALUES
    (100, 'residual', 'r100'),
    (101, 'residual', 'r101'),
    (102, 'residual', 'r102');

UPDATE default.spark_rewrite_mor_residual_deletes
SET payload = CASE
    WHEN id = 2 THEN 'c2_u'
    WHEN id = 101 THEN 'r101_u'
    ELSE payload
END
WHERE id IN (2, 101);

DELETE FROM default.spark_rewrite_mor_residual_deletes
WHERE id IN (3, 102);
