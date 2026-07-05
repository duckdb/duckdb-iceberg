from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.day_timestamp",
    """
CREATE OR REPLACE TABLE default.day_timestamp (
    partition_col TIMESTAMP_NTZ,
    user_id BIGINT,
    event_type STRING
)
USING iceberg
PARTITIONED BY (day(partition_col))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read'
);

INSERT INTO default.day_timestamp VALUES
    (TIMESTAMP_NTZ '2020-05-15 14:30:45', 12345, 'click'),
    (TIMESTAMP_NTZ '2021-08-22 09:15:20', 67890, 'purchase'),
    (TIMESTAMP_NTZ '2022-03-10 11:45:30', 54321, 'view');

INSERT INTO default.day_timestamp VALUES
    (NULL, 99999, 'null_event');

INSERT INTO default.day_timestamp VALUES
    (TIMESTAMP_NTZ '2023-01-01 00:00:00', NULL, 'null_user'),
    (TIMESTAMP_NTZ '2023-02-15 12:30:45', 88888, NULL);
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
@pytest.mark.generator_catalog("local")
def test_day_timestamp(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        initialize=False,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("day_timestamp.test"))
