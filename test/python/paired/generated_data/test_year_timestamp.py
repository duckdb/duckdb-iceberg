from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.spark_seed_tables("default.year_timestamp")
@pytest.mark.generator_catalog("local")
def test_year_timestamp(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("year_timestamp.test"))
