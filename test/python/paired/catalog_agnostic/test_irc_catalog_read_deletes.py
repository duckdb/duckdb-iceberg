import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import RegisteredSeedTable


@pytest.mark.spark_seed_tables(
    RegisteredSeedTable("default.lineitem_001_deletes", write_intermediates=True),
    RegisteredSeedTable("default.lineitem_sf1_deletes", write_intermediates=True),
    RegisteredSeedTable("default.lineitem_sf_01_no_deletes", write_intermediates=True),
    RegisteredSeedTable("default.lineitem_sf_01_1_delete", write_intermediates=True),
    RegisteredSeedTable("default.lineitem_partitioned_l_shipmode", write_intermediates=True),
    RegisteredSeedTable("default.lineitem_partitioned_l_shipmode_deletes", write_intermediates=True),
)
def test_irc_catalog_read_deletes(
    paired_sqllogic_test_path,
    unittest_binary,
    unittest_test_config,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        test_config=unittest_test_config,
        print_stdin=print_unittest_stdin,
    ) as runner:
        runner.run_sqllogic_file(paired_sqllogic_test_path)
