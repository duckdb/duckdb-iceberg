UPDATE default.row_lineage_test_upgraded
SET data = CONCAT(data, '_u1')
WHERE id IN (2, 4);
