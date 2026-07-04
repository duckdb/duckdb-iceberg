from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner


GENERATED_DATA_ENV = {"DUCKDB_ICEBERG_HAVE_GENERATED_DATA": "1"}


@pytest.mark.spark_seed_tables("default.pyspark_iceberg_table_v1")
@pytest.mark.generator_catalog("local")
def test_iceberg_scan(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        env=GENERATED_DATA_ENV,
        initialize=False,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("iceberg_scan.test"))
