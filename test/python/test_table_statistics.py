from functools import partial

import pytest

from conftest import is_active_catalog
from duckdb_unittest import DuckDBUnittestRunner


JAVA_FIXTURE_ONLY = pytest.mark.skipif(
    not is_active_catalog("fixture"),
    reason="Registers a statistics file through pyiceberg against the Java REST fixture",
)

THETA_SKETCH_BLOB_TYPE = "apache-datasketches-theta-v1"


@pytest.fixture
def duckdb_runner(unittest_binary, unittest_test_config, print_unittest_stdin):
    return partial(
        DuckDBUnittestRunner,
        unittest_binary,
        test_config=unittest_test_config,
        print_stdin=print_unittest_stdin,
    )


def _drop_table_if_exists(catalog, identifier):
    if catalog.table_exists(identifier):
        catalog.drop_table(identifier)


def _without_distinct_count(table_name, column):
    return f"SELECT stats({column})::VARCHAR NOT LIKE '%approx_unique%' FROM {table_name} LIMIT 1"


def _distinct_count(table_name, column, at=""):
    return f"SELECT s.approx_unique::BIGINT FROM (SELECT stats({column}) s FROM {table_name} {at} LIMIT 1)"


@JAVA_FIXTURE_ONLY
def test_scan_uses_the_distinct_counts_of_the_statistics_file(rest_catalog, duckdb_runner):
    statistics = pytest.importorskip("pyiceberg.table.statistics")
    identifier = "default.scan_table_statistics"
    table_name = f"my_datalake.{identifier}"
    _drop_table_if_exists(rest_catalog, identifier)
    try:
        with duckdb_runner() as test:
            test.statement_ok(f"CREATE TABLE {table_name} (id INTEGER, category VARCHAR)")
            test.statement_ok(f"INSERT INTO {table_name} SELECT range, 'c' || (range % 3) FROM range(10)")
            test.query("I", _without_distinct_count(table_name, "category"), [[True]])

        # The statistics file another engine computed for the current snapshot: 3 distinct categories.
        table = rest_catalog.load_table(identifier)
        snapshot_id = table.current_snapshot().snapshot_id
        category_field = table.schema().find_field("category").field_id
        table.update_statistics().set_statistics(
            statistics.StatisticsFile(
                snapshot_id=snapshot_id,
                statistics_path=f"{table.location()}/metadata/{snapshot_id}.stats",
                file_size_in_bytes=180,
                file_footer_size_in_bytes=167,
                blob_metadata=[
                    statistics.BlobMetadata(
                        type=THETA_SKETCH_BLOB_TYPE,
                        snapshot_id=snapshot_id,
                        sequence_number=1,
                        fields=[category_field],
                        properties={"ndv": "3"},
                    )
                ],
            )
        ).commit()

        with duckdb_runner() as test:
            test.query("I", _distinct_count(table_name, "category"), [[3]])
            test.query("I", _without_distinct_count(table_name, "id"), [[True]])
            # A new snapshot has no statistics file; the previous snapshot keeps its counts.
            test.statement_ok(f"INSERT INTO {table_name} VALUES (10, 'c3')")
            test.query("I", _without_distinct_count(table_name, "category"), [[True]])
            test.query("I", _distinct_count(table_name, "category", f"AT (VERSION => {snapshot_id})"), [[3]])
    finally:
        _drop_table_if_exists(rest_catalog, identifier)
