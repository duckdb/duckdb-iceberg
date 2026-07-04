import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.test_not_null",
    """
CREATE OR REPLACE TABLE default.test_not_null (
    id INT,
    name STRING NOT NULL,
    address STRUCT<
        street: STRING NOT NULL,
        city: STRING NOT NULL,
        zip: STRING NOT NULL
    >,
    phone_numbers ARRAY<STRING>,
    metadata MAP<STRING, STRING>
);

INSERT INTO default.test_not_null VALUES (
    1,
    'Alice',
    NAMED_STRUCT('street', '123 Main St', 'city', 'Metropolis', 'zip', '12345'),
    ARRAY('123-456-7890', '987-654-3210'),
    MAP('age', '30', 'membership', 'gold')
);
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
def test_insert_into_null_columns(
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
