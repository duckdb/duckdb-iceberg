UPDATE default.row_lineage_test
SET data = CONCAT(data, '_u1')
WHERE id IN (2, 4);
