from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.spark_seed_tables("default.schema_evolve_struct_in_list")
@pytest.mark.generator_catalog("local")
def test_schema_evolve_struct_in_list(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("schema_evolve_struct_in_list.test"))
