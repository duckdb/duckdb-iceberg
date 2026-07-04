from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import RegisteredSeedTable


@pytest.mark.spark_seed_tables(
    RegisteredSeedTable("default.pyspark_iceberg_table_v1", write_intermediates=True),
    RegisteredSeedTable("default.pyspark_iceberg_table_v2", write_intermediates=True),
)
@pytest.mark.generator_catalog("local")
def test_iceberg_scan_generated_data_0_001(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        initialize=False,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("iceberg_scan_generated_data_0_001.test"))
