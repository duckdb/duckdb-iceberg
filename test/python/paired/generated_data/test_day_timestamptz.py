from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.day_timestamptz",
    """
CREATE OR REPLACE TABLE default.day_timestamptz (
    partition_col TIMESTAMP,
    user_id BIGINT,
    event_type STRING
)
USING iceberg
PARTITIONED BY (day(partition_col))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read'
);

INSERT INTO default.day_timestamptz VALUES
    (TIMESTAMP '2020-05-15 14:30:45', 12345, 'click'),
    (TIMESTAMP '2021-08-22 09:15:20', 67890, 'purchase'),
    (TIMESTAMP '2022-03-10 11:45:30', 54321, 'view');
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
@pytest.mark.generator_catalog("local")
def test_day_timestamptz(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        initialize=False,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("day_timestamptz.test"))
