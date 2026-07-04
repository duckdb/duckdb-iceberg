import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.empty_table",
    """
CREATE or REPLACE TABLE default.empty_table (
     col1 date,
     col2 integer,
     col3 string
)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
def test_cascade(
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
