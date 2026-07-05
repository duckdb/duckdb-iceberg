import pytest

from duckdb_unittest import DuckDBUnittestRunner


TPCH_PREAMBLE = """
require-env CATALOG_TEST_CONFIG_SETUP

require avro

require parquet

require iceberg

require httpfs

require core_functions

require tpch

# Do not ignore 'HTTP' error messages!
set ignore_error_messages

statement ok
set enable_logging=true

statement ok
set logging_level='debug'
"""


@pytest.mark.spark_seed_tables("default.tpch")
def test_tpch(
    paired_sqllogic_test_path,
    unittest_binary,
    unittest_test_config,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        test_config=unittest_test_config,
        print_stdin=print_unittest_stdin,
        preamble=TPCH_PREAMBLE,
    ) as runner:
        runner.run_sqllogic_file(paired_sqllogic_test_path)
