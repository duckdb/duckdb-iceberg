from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.schema_evolve_int_to_bigint",
    """
CREATE or REPLACE TABLE default.schema_evolve_int_to_bigint (
	col integer
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);

INSERT INTO default.schema_evolve_int_to_bigint
VALUES
	(-2147483648),
	(-1),
	(0),
	(1),
	(2147483647);

ALTER TABLE default.schema_evolve_int_to_bigint ALTER col TYPE BIGINT;

INSERT INTO default.schema_evolve_int_to_bigint
VALUES
	(-9223372036854775808),
	(-1),
	(0),
	(1),
	(9223372036854775807);
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
@pytest.mark.generator_catalog("local")
def test_schema_evolve_int_to_bigint(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        initialize=False,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("schema_evolve_int_to_bigint.test"))
