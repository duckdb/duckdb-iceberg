from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner


GENERATED_DATA_ENV = {"DUCKDB_ICEBERG_HAVE_GENERATED_DATA": "1"}


@pytest.mark.spark_seed_tables("default.year_timestamptz")
@pytest.mark.generator_catalog("local")
def test_day_timestamptz(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        env=GENERATED_DATA_ENV,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("day_timestamptz.test"))
