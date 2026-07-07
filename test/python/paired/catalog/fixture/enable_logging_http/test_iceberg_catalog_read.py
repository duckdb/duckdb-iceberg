from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.requires_catalog("fixture")
@pytest.mark.spark_seed_tables(
    "default.table_unpartitioned",
    "default.table_more_deletes",
    "default.pyspark_iceberg_table_v2",
)
def test_iceberg_catalog_read(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        use_test_config=False,
        preamble="",
        print_stdin=print_unittest_stdin,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("iceberg_catalog_read.test"))
