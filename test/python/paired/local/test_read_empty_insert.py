from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.empty_insert",
    """
CREATE or REPLACE TABLE default.empty_insert (
     col1 date,
     col2 integer,
     col3 string
)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);

INSERT INTO default.empty_insert
SELECT DATE '2010-06-11' col1, 42 col2, 'test' col3 WHERE 1 = 0
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
@pytest.mark.generator_catalog("local")
def test_read_empty_insert(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        initialize=False,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("test_read_empty_insert.test"))
