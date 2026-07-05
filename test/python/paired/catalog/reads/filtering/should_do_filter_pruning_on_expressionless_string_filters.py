from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.spark_seed_tables("default.issue_791")
def test_should_do_filter_pruning_on_expressionless_string_filters(
    unittest_binary, unittest_test_config, print_unittest_stdin
):
    with DuckDBUnittestRunner(
        unittest_binary,
        test_config=unittest_test_config,
        print_stdin=print_unittest_stdin,
    ) as runner:
        runner.run_sqllogic_file(
            Path(__file__).with_name("should_do_filter_pruning_on_expressionless_string_filters.test")
        )
