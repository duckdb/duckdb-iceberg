from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.schema_evolve_widen_decimal",
    """
CREATE or REPLACE TABLE default.schema_evolve_widen_decimal (
	col DECIMAL(12, 8)
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);

INSERT INTO default.schema_evolve_widen_decimal
VALUES
	(1234.12345678),
	(987.87654321),
	(12.12345678);

ALTER TABLE default.schema_evolve_widen_decimal 
	ALTER COLUMN col TYPE DECIMAL(18, 8);

INSERT INTO default.schema_evolve_widen_decimal
VALUES
	(1234567890.12345678),
	(987654321.98765432),
	(123.12345678);
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
@pytest.mark.generator_catalog("local")
def test_schema_evolve_widen_decimal(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        initialize=False,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("schema_evolve_widen_decimal.test"))
