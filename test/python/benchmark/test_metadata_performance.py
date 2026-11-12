from pathlib import Path

from duckdb_unittest import DuckDBUnittestRunner
from scripts.metadata_performance.workloads import (
    SHAPES,
    delete_cases,
    delete_setup,
    expected_layout,
    layout_query,
    manifest_case,
    manifest_setup,
)


def setup_benchmarks(test):
    test.statement_ok("SET threads=1")
    shape = SHAPES["regression"]
    for with_deletes in (True, False):
        table = "my_datalake.default.metadata_delete_" + ("baseline" if with_deletes else "control")
        test.statement_ok(f"DROP TABLE IF EXISTS {table}")
        for sql in delete_setup(shape, table, with_deletes):
            test.statement_ok(sql)
        layout = expected_layout(shape, with_deletes)
        with test.with_transaction():
            test.query("IIIIII", layout_query(table), [tuple(layout.values())])
        test.query("I", f"SELECT count(*) FROM {table}", [(layout["data_rows"] - layout["live_delete_entries"],)])
        for case in delete_cases(shape, table, with_deletes):
            test.statement_ok(case["keys_sql"])
            test.query("I", case["sql"], [(case["expected"],)])

    table = "my_datalake.default.metadata_manifest_1000"
    test.statement_ok(f"DROP TABLE IF EXISTS {table}")
    for sql in manifest_setup(1000, table):
        test.statement_ok(sql)
    with test.with_transaction():
        test.query("IIIIII", layout_query(table), [(1000, 1000, 0, 1, 0, 0)])
    test.query("I", f"SELECT count(*) FROM {table}", [(1000,)])
    test.query("I", manifest_case(table), [(1,)])


def test_setup_metadata_performance_benchmark(
    catalog_profile,
    unittest_binary,
    unittest_test_config,
    duckdb_catalog_init_sql,
    print_unittest_stdin,
):
    assert catalog_profile.name in ("fixture", "fixture-latest")
    with DuckDBUnittestRunner(
        unittest_binary, test_config=unittest_test_config, print_stdin=print_unittest_stdin
    ) as test:
        setup_benchmarks(test)

    # Regression runner load/init/run blocks share one attachment. Resolve its
    # initialization from the existing catalog profile, just like unittest setup.
    directory = Path(__file__).resolve().parents[3] / ".catalogs/metadata-performance-regression"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "load.benchmark").write_text(
        "load\n"
        + duckdb_catalog_init_sql.replace("\n", " ")
        + "\nSET iceberg_use_server_side_scan_planning=false;\nSET enable_external_file_cache=true;\n"
    )
