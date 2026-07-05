from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.schema_evolve_struct",
    """
CREATE OR REPLACE TABLE default.schema_evolve_struct (
	user_id INT,
	user_details STRUCT<first_name: STRING, last_name: STRING>,
	tags ARRAY<INT>,
	preferences MAP<STRING, INT>
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);

INSERT INTO default.schema_evolve_struct VALUES
	(1, NAMED_STRUCT(
		'first_name', 'Alice',
		'last_name', 'Smith'),
		ARRAY(21, 42),
		MAP('theme', 131, 'language', 115)
	),
	(2, NAMED_STRUCT(
		'first_name', 'Bob',
		'last_name', 'Jones'),
		ARRAY(13, 24),
		MAP('theme', 1, 'language', 1337)
	);

ALTER TABLE default.schema_evolve_struct
ADD COLUMNS (
	user_details.email STRING
);

INSERT INTO default.schema_evolve_struct VALUES
    (3, NAMED_STRUCT(
        'first_name', 'Charlie',
        'last_name', 'Brown',
        'email', 'charlie@example.com'),
        ARRAY(34, 65),
        MAP('theme', 104, 'theme_importance', 3, 'language', 13, 'language_importance', 2)
    ),
    (4, NAMED_STRUCT(
        'first_name', 'Diana',
        'last_name', 'Prince',
        'email', 'diana@example.com'),
        ARRAY(45, 93),
        MAP('theme', 12, 'theme_importance', 1, 'language', 213, 'language_importance', 3)
    );

ALTER TABLE default.schema_evolve_struct
	RENAME COLUMN user_details.first_name TO given_name;

INSERT INTO default.schema_evolve_struct VALUES
	(5, NAMED_STRUCT(
		'given_name', 'Eve',
		'last_name', 'Doe',
		'email', 'eve@example.com'),
		ARRAY(200, 300),
		MAP('setting', 234, 'value', 235, 'value_importance', 2)
	);

ALTER TABLE default.schema_evolve_struct
ADD COLUMNS (
	user_details.age INT
);

INSERT INTO default.schema_evolve_struct VALUES
	(6, NAMED_STRUCT(
		'given_name', 'Frank',
		'last_name', 'Miller',
		'email', 'frank@example.com',
		'age', 42),
		ARRAY(500, 501),
		MAP('setting', 324, 'value', 167, 'value_importance', 2, 'value_updated_at', 123213)
	);

ALTER TABLE default.schema_evolve_struct
ALTER COLUMN user_details.age TYPE BIGINT;

ALTER TABLE default.schema_evolve_struct
ALTER COLUMN tags.element TYPE BIGINT;

ALTER TABLE default.schema_evolve_struct
ALTER COLUMN preferences.value TYPE BIGINT;

INSERT INTO default.schema_evolve_struct VALUES
	(7, NAMED_STRUCT(
		'given_name', 'Grace',
		'last_name', 'Hopper',
		'email', 'grace@example.com',
		'age', 9223372036854775806),
		ARRAY(300, 9223372036854775806, 1000),
		MAP('setting', 9223372036854775806, 'negative', -9223372036854775807)
	);

ALTER TABLE default.schema_evolve_struct
DROP COLUMN user_details.last_name;

INSERT INTO default.schema_evolve_struct VALUES
	(8, NAMED_STRUCT(
		'given_name', 'Heidi',
		'email', 'heidi@example.com',
		'age', 30),
		ARRAY(1, 2, 3, 4, 5),
		MAP('setting', 245, 'value', 453, 'value_updated_at', 3253)
	);
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
@pytest.mark.generator_catalog("local")
def test_schema_evolve_struct(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        initialize=False,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("schema_evolve_struct.test"))
