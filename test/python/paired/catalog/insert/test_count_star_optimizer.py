import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.count_star_optimizer",
    """
CREATE OR REPLACE TABLE default.count_star_optimizer (
    id INT,
    name STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.data.partition-columns' = false,
    'write.parquet.write-partition-values' = false
);

INSERT INTO default.count_star_optimizer VALUES  (1,'aaa'), (2,'bbb');
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
def test_count_star_optimizer(
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
