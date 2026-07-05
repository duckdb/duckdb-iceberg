import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.column_doc_comment",
    """
CREATE OR REPLACE TABLE default.column_doc_comment(
  id bigint COMMENT 'Primary identifier',
  population bigint COMMENT 'Resident count, 2024 census',
  name string
)
TBLPROPERTIES (
    'format-version'='2'
);

insert into default.column_doc_comment values
(1, 1000, 'alpha'),
(2, 2000, 'beta');
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
def test_column_doc_comment(
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
