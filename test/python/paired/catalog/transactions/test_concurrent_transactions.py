import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.quickstart_table",
    """
CREATE OR REPLACE TABLE default.quickstart_table (
     id BIGINT, data STRING
)
USING ICEBERG;

INSERT INTO default.quickstart_table VALUES
	(1, 'some data'),
	(2, 'more data'),
	(3, 'yet more data');
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
def test_concurrent_transactions(
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
