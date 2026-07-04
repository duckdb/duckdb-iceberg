import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.test_null_filtering",
    """
CREATE or REPLACE TABLE default.test_null_filtering (a int, b VARCHAR(20))
TBLPROPERTIES (
	'format-version' = '2',
	'write.delete.mode' = 'merge-on-read',
	'write.update.mode' = 'merge-on-read'
);

insert into default.test_null_filtering values
	(0, 'hello'),
	(1, 'world'),
	(null, 'duck'),
	(3, null);

insert into default.test_null_filtering values
	(5, 'wonderful'),
	(6, 'ducks');

insert into default.test_null_filtering values
	(null, null);

insert into default.test_null_filtering values
    (10, 'no nulls');
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
def test_read_nulls(
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
