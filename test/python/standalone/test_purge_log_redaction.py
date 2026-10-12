"""Files that cannot be deleted are logged, not raised, and the log honors redact_http_logs.

Both the client-side purge of a dropped table (PURGE_MODE 'client') and the cleanup of a rolled-back transaction
delete files best effort, like Iceberg's CatalogUtil.dropTableData: the DROP TABLE and the rollback succeed, and the
files that stay behind are named in a warning, redacted unless redact_http_logs is off. The deletion is made to
fail by taking the write permission of the data directory away, which a sqllogictest cannot do, hence this test
drives the unittest binary from Python against its own mock REST catalog.
"""

import json
import os
import time
from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from scripts.mock_rest_catalog.server import MockServer


pytestmark = pytest.mark.skipif(
    os.name == "nt" or (hasattr(os, "geteuid") and os.geteuid() == 0),
    reason="chmod cannot reliably make deletion fail on this platform/user",
)


def wait_for(predicate, description: str, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"Timed out waiting for {description}")


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sync_runner(test: DuckDBUnittestRunner, marker: Path) -> None:
    test.statement_ok(f"COPY (SELECT 1) TO {sql_string(marker)} (FORMAT csv)")
    wait_for(marker.exists, f"DuckDB marker {marker.name}")


def table_location(server: MockServer, table: str) -> Path:
    key = (("default",), table)

    def published():
        with server.catalog.lock:
            return key in server.catalog.tables

    wait_for(published, f"mock table default.{table}")
    with server.catalog.lock:
        return Path(server.catalog.tables[key]["metadata"]["location"])


def data_files(location: Path, expected: int) -> list[Path]:
    def files():
        return sorted((location / "data").glob("*.parquet"))

    wait_for(lambda: len(files()) == expected, f"{expected} data files under {location}")
    return files()


def any_path_predicate(paths: list[Path]) -> str:
    return "(" + " OR ".join(f"contains(message, {sql_string(path)})" for path in paths) + ")"


def all_paths_predicate(paths: list[Path]) -> str:
    return "(" + " AND ".join(f"contains(message, {sql_string(path)})" for path in paths) + ")"


def detail_predicate(paths: list[Path], *, redact: bool) -> str:
    if redact:
        return "contains(message, '[redacted]: redacted')"
    return all_paths_predicate(paths)


def assert_failure_logged(
    test: DuckDBUnittestRunner, paths: list[Path], *, operation: str, description: str, redact: bool
) -> None:
    """One warning names the batch; with redaction on, no log entry at all names a file."""
    prefix = f"{operation}, failed to delete one or more of 2 {description}(s) ["
    test.query(
        "I",
        f"""
        SELECT count(*) FROM duckdb_logs
        WHERE type = '' AND log_level = 'WARNING'
          AND contains(message, {sql_string(prefix)})
          AND {detail_predicate(paths, redact=redact)}
        """,
        [(1,)],
    )
    if redact:
        test.query("I", f"SELECT count(*) FROM duckdb_logs WHERE {any_path_predicate(paths)}", [(0,)])


def start_logging(test: DuckDBUnittestRunner, tmp_path: Path, table: str, *, redact: bool) -> None:
    test.statement_ok(f"SET redact_http_logs={str(redact).lower()}")
    test.statement_ok("CALL truncate_duckdb_logs()")
    sync_runner(test, tmp_path / f"{table}-logs-ready.csv")


def run_purge_failure_case(
    test: DuckDBUnittestRunner,
    server: MockServer,
    tmp_path: Path,
    table: str,
    *,
    redact: bool,
) -> None:
    test.statement_ok(f"CREATE TABLE purge_catalog.default.{table} (a INTEGER)")
    test.statement_ok(f"INSERT INTO purge_catalog.default.{table} VALUES (1)")
    test.statement_ok(f"INSERT INTO purge_catalog.default.{table} VALUES (2)")
    sync_runner(test, tmp_path / f"{table}-prepared.csv")
    location = table_location(server, table)
    paths = data_files(location, 2)
    data_directory = paths[0].parent
    start_logging(test, tmp_path, table, redact=redact)

    data_directory.chmod(0o500)
    try:
        # The catalog accepted the drop; the files it leaves behind do not fail the statement.
        test.statement_ok(f"DROP TABLE purge_catalog.default.{table}")
        sync_runner(test, tmp_path / f"{table}-drop-completed.csv")
    finally:
        data_directory.chmod(0o700)

    test.statement_error(f"SELECT * FROM purge_catalog.default.{table}", "does not exist")
    # The data files stayed behind, every later group was still deleted.
    assert all(path.exists() for path in paths)
    assert not list((location / "metadata").iterdir())
    assert_failure_logged(
        test,
        paths,
        operation=f'Purge of Iceberg table "purge_catalog"."default"."{table}"',
        description="data file",
        redact=redact,
    )


def run_rollback_failure_case(
    test: DuckDBUnittestRunner,
    server: MockServer,
    tmp_path: Path,
    table: str,
    *,
    redact: bool,
) -> None:
    test.statement_ok(f"CREATE TABLE purge_catalog.default.{table} (a INTEGER)")
    location = table_location(server, table)
    test.statement_ok("BEGIN")
    test.statement_ok(f"INSERT INTO purge_catalog.default.{table} VALUES (1)")
    test.statement_ok(f"INSERT INTO purge_catalog.default.{table} VALUES (2)")
    sync_runner(test, tmp_path / f"{table}-prepared.csv")
    paths = data_files(location, 2)
    data_directory = paths[0].parent
    start_logging(test, tmp_path, table, redact=redact)

    data_directory.chmod(0o500)
    try:
        test.statement_ok("ABORT")
        sync_runner(test, tmp_path / f"{table}-abort-completed.csv")
    finally:
        data_directory.chmod(0o700)

    assert all(path.exists() for path in paths)
    assert_failure_logged(test, paths, operation="Iceberg Transaction Cleanup", description="data_file", redact=redact)
    test.query("I", f"SELECT count(*) FROM purge_catalog.default.{table}", [(0,)])
    test.statement_ok(f"DROP TABLE purge_catalog.default.{table}")


def test_failed_file_cleanup_is_logged_with_redaction(tmp_path, unittest_binary, print_unittest_stdin):
    catalog_directory = tmp_path / "catalog"
    catalog_directory.mkdir()
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "statically_loaded_extensions": ["core_functions", "parquet", "avro", "iceberg", "httpfs"],
                "test_env": [{"env_name": "CATALOG_TEST_CONFIG_SETUP", "env_value": "purge-redaction"}],
            }
        )
    )

    with MockServer(catalog_directory) as server:
        with DuckDBUnittestRunner(
            unittest_binary,
            test_config=config,
            print_stdin=print_unittest_stdin,
        ) as test:
            test.statement_ok("CALL disable_logging()")
            test.statement_ok("CALL enable_logging(level='debug', storage='memory')")
            test.statement_ok(
                f"""
                ATTACH '' AS purge_catalog (
                    TYPE ICEBERG,
                    URI '{server.uri}',
                    AUTHORIZATION_TYPE 'none',
                    ACCESS_DELEGATION_MODE 'none',
                    STAGE_CREATE_TABLES true,
                    PURGE_MODE 'client'
                )
                """
            )
            test.statement_ok("CREATE SCHEMA purge_catalog.default")

            run_purge_failure_case(test, server, tmp_path, "purge_redacted", redact=True)
            run_purge_failure_case(test, server, tmp_path, "purge_unredacted", redact=False)
            run_rollback_failure_case(test, server, tmp_path, "rollback_redacted", redact=True)
            run_rollback_failure_case(test, server, tmp_path, "rollback_unredacted", redact=False)
