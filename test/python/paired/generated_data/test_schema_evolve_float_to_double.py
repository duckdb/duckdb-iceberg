from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.schema_evolve_float_to_double",
    """
CREATE or REPLACE TABLE default.schema_evolve_float_to_double (
	col float
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);

INSERT INTO default.schema_evolve_float_to_double
VALUES
	(1.23),
	(4.56),
	(7.89);

ALTER TABLE default.schema_evolve_float_to_double
	ALTER COLUMN col TYPE DOUBLE;

INSERT INTO default.schema_evolve_float_to_double
VALUES
	(1.23456789),
	(3.141592653589793),
	(2.718281828459045);
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
@pytest.mark.generator_catalog("local")
def test_schema_evolve_float_to_double(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        initialize=False,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("schema_evolve_float_to_double.test"))
