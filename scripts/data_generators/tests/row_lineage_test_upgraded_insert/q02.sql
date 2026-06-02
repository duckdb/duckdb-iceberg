UPDATE default.row_lineage_test_upgraded_insert
SET data = CONCAT(data, '_u1')
WHERE id IN (2, 4);
