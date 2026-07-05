from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.nested_types",
    """
CREATE OR REPLACE TABLE default.nested_types (
    id INT,
    name STRING,
    address STRUCT<
        street: STRING,
        city: STRING,
        zip: STRING
    >,
    phone_numbers ARRAY<STRING>,
    metadata MAP<STRING, STRING>
);

INSERT INTO default.nested_types VALUES (
  1,
  'Alice',
  NAMED_STRUCT('street', '123 Main St', 'city', 'Metropolis', 'zip', '12345'),
  ARRAY('123-456-7890', '987-654-3210'),
  MAP('age', '30', 'membership', 'gold')
);
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
@pytest.mark.generator_catalog("local")
def test_nested_types(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        initialize=False,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("nested_types.test"))
