from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.schema_evolve_struct_in_list",
    """
CREATE OR REPLACE TABLE default.schema_evolve_struct_in_list (
	tags ARRAY<STRUCT<first_name: STRING, age: INTEGER>>
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);

INSERT INTO default.schema_evolve_struct_in_list VALUES
	(ARRAY(
		NAMED_STRUCT(
			'first_name', 'Alice',
			'age', 43
		),
		NAMED_STRUCT(
			'first_name', 'Bob',
			'age', 35
		)
	));

ALTER TABLE default.schema_evolve_struct_in_list
ALTER COLUMN tags.element.age TYPE BIGINT;

INSERT INTO default.schema_evolve_struct_in_list VALUES
	(ARRAY(
		NAMED_STRUCT(
			'first_name', 'Ancient Being',
			'age', 9223372036854775807
		),
		NAMED_STRUCT(
			'first_name', 'Bobby Droptables',
			'age', 2147483649
		)
	));

ALTER TABLE default.schema_evolve_struct_in_list
ADD COLUMNS (
	tags.element.last_name STRING
);

INSERT INTO default.schema_evolve_struct_in_list VALUES
	(ARRAY(
		NAMED_STRUCT(
			'first_name', 'Hello',
			'age', 9223372036854775807,
			'last_name', 'World'
		)
	));

ALTER TABLE default.schema_evolve_struct_in_list
RENAME COLUMN tags.element.first_name TO given_name;

INSERT INTO default.schema_evolve_struct_in_list VALUES
	(ARRAY(
		NAMED_STRUCT(
			'given_name', 'Duck',
			'age', 5,
			'last_name', 'DB'
		)
	));

ALTER TABLE default.schema_evolve_struct_in_list
DROP COLUMN tags.element.last_name;
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
@pytest.mark.generator_catalog("local")
def test_schema_evolve_struct_in_list(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        initialize=False,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("schema_evolve_struct_in_list.test"))
