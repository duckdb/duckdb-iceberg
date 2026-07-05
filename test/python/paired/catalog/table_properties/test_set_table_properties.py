import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.test_table_properties",
    """
CREATE or REPLACE TABLE default.test_table_properties (a int)
TBLPROPERTIES (
	'format-version' = '2',
	'write.delete.mode' = 'merge-on-read'
);

INSERT INTO default.test_table_properties VALUES
	(1),
	(2),
	(3);

ALTER TABLE default.test_table_properties SET TBLPROPERTIES (
    'read.split.target-size'='268435456',
    'foo'='baz'
);

ALTER TABLE default.test_table_properties UNSET TBLPROPERTIES ('foo');

ALTER TABLE default.test_table_properties SET TBLPROPERTIES (
    'duckdb_man'='superman'
);

INSERT INTO default.test_table_properties VALUES
	(1),
	(2),
	(3);

ALTER TABLE default.test_table_properties SET TBLPROPERTIES (
    'another1'='neato'
);
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
def test_set_table_properties(
    paired_sqllogic_test_path,
    unittest_binary,
    unittest_test_config,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        test_config=unittest_test_config,
        print_stdin=print_unittest_stdin,
    ) as runner:
        runner.run_sqllogic_file(paired_sqllogic_test_path)
