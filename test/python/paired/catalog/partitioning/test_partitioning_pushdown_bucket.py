import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.spark_seed_tables(
    "default.bucket_partitioned_integer",
    "default.bucket_partitioned_bigint",
    "default.bucket_partitioned_string",
    "default.bucket_partitioned_date",
    "default.bucket_partitioned_decimal",
    "default.bucket_partitioned_binary",
    "default.bucket_partitioned_timestamp",
    "default.bucket_partitioned_sparse_integer",
)
def test_partitioning_pushdown_bucket(
    paired_sqllogic_test_path,
    unittest_binary,
    unittest_test_config,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        test_config=unittest_test_config,
        preamble='',
        print_stdin=print_unittest_stdin,
    ) as runner:
        runner.run_sqllogic_file(paired_sqllogic_test_path)
