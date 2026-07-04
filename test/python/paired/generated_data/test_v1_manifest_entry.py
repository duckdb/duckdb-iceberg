from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.spark_seed_tables("default.test_add_files")
@pytest.mark.generator_catalog("local")
def test_v1_manifest_entry(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        initialize=False,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("test_v1_manifest_entry.test"))
