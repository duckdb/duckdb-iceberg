import json
from contextlib import contextmanager
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from time import sleep
from uuid import uuid4

import pytest

from conftest import is_active_catalog
from duckdb_unittest import DuckDBUnittestRunner


JAVA_FIXTURE_ONLY = pytest.mark.skipif(
    not is_active_catalog("fixture"),
    reason="Requires Apache Iceberg Java REST fixture metadata registration/update behavior",
)
NESSIE_UNSUPPORTED = pytest.mark.skipif(
    is_active_catalog("nessie"),
    reason="Nessie sets gc.enabled=false and rejects external snapshot management through remove-snapshots",
)


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


def _expire_query(table_name, *options):
    arguments = ", ".join((f"'{table_name}'", *options))
    return f"SELECT * FROM iceberg_expire_snapshots({arguments})"


def _check_expiration(test, table_name, expected, *options, connection=None):
    test.query("II", _expire_query(table_name, *options), [expected], connection=connection)


def _timestamp_time_travel_query(table_name, timestamp_ms):
    return f"SELECT count(*) FROM {table_name} AT (TIMESTAMP => make_timestamp_ms({timestamp_ms}))"


@contextmanager
def _serve_table_metadata(metadata_location, metadata):
    class MetadataHandler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.send_response(200)
            self.end_headers()

        def do_GET(self):
            if self.path.startswith("/v1/config"):
                self._respond({"defaults": {}, "overrides": {}})
            elif "/namespaces/" in self.path and "/tables/" in self.path:
                self._respond({"metadata-location": metadata_location, "metadata": metadata, "config": {}})
            else:
                self._respond({}, status=404)

        def _respond(self, body, status=200):
            encoded = json.dumps(body, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), MetadataHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def _attach_metadata_catalog(test, catalog_name, port):
    test.statement_ok(
        f"""
        ATTACH '' AS {catalog_name} (
            TYPE ICEBERG,
            URI 'http://127.0.0.1:{port}',
            AUTHORIZATION_TYPE 'none'
        )
        """
    )


@pytest.fixture
def seeded_table(rest_catalog, duckdb_runner):
    identifiers = []

    def seed(identifier, count, *, distinct_timestamps=False):
        _drop_table_if_exists(rest_catalog, identifier)
        identifiers.append(identifier)
        table_name = f"my_datalake.{identifier}"
        if distinct_timestamps:
            with duckdb_runner() as test:
                test.statement_ok(f"CREATE TABLE {table_name} (id INTEGER)")
            for value in range(1, count + 1):
                with duckdb_runner() as test:
                    test.statement_ok(f"INSERT INTO {table_name} VALUES ({value})")
                # REST commits normally take longer than a millisecond, but make
                # the timestamp-gap regression independent of that latency.
                sleep(0.01)
        else:
            with duckdb_runner() as test:
                test.statement_ok(f"CREATE TABLE {table_name} (id INTEGER)")
                for value in range(1, count + 1):
                    test.statement_ok(f"INSERT INTO {table_name} VALUES ({value})")
        return rest_catalog.load_table(identifier), table_name

    yield seed

    for identifier in identifiers:
        _drop_table_if_exists(rest_catalog, identifier)


def _ordered_snapshots(table):
    return sorted(table.metadata.snapshots, key=lambda snapshot: snapshot.sequence_number)


@pytest.fixture
def registered_metadata_table(rest_catalog, seeded_table):
    registered_identifiers = []

    def register(source_identifier, target_identifier, snapshot_count, mutate_metadata):
        source_table, _ = seeded_table(source_identifier, snapshot_count)
        _drop_table_if_exists(rest_catalog, target_identifier)
        metadata = source_table.metadata.model_dump(mode="json", by_alias=True, exclude_none=True)
        mutate_metadata(metadata)

        metadata_directory = source_table.metadata_location.rsplit("/", 1)[0]
        metadata_location = f"{metadata_directory}/synthetic-{uuid4()}.metadata.json"
        with source_table.io.new_output(metadata_location).create() as output_stream:
            output_stream.write(json.dumps(metadata, separators=(",", ":")).encode("utf-8"))

        registered_identifiers.append(target_identifier)
        table = rest_catalog.register_table(target_identifier, metadata_location)
        return table, f"my_datalake.{target_identifier}"

    yield register

    for identifier in registered_identifiers:
        _drop_table_if_exists(rest_catalog, identifier)


@NESSIE_UNSUPPORTED
class TestExpireSnapshots:
    def test_cutoff_precision(self, seeded_table, duckdb_runner):
        table, table_name = seeded_table("default.expire_snapshots_main_only", 5)
        snapshots = _ordered_snapshots(table)
        oldest_timestamp_ms = min(snapshot.timestamp_ms for snapshot in snapshots)
        snapshots_before_cutoff = sum(
            snapshot.timestamp_ms == oldest_timestamp_ms
            and snapshot.snapshot_id != table.current_snapshot().snapshot_id
            for snapshot in snapshots
        )

        with duckdb_runner() as test:
            test.statement_ok("BEGIN")
            _check_expiration(
                test,
                table_name,
                (0, 5),
                "retain_last => 1",
                f"older_than => make_timestamp_ms({oldest_timestamp_ms})",
            )
            _check_expiration(
                test,
                table_name,
                (snapshots_before_cutoff, 5 - snapshots_before_cutoff),
                "retain_last => 1",
                f"older_than => make_timestamp_ms({oldest_timestamp_ms}) + interval 1 microsecond",
            )
            test.statement_ok("ROLLBACK")

    @JAVA_FIXTURE_ONLY
    def test_non_main_refs_report_first_name_without_staging(
        self,
        registered_metadata_table,
        duckdb_runner,
    ):
        def add_non_main_refs(metadata):
            current_id = metadata["current-snapshot-id"]
            metadata["refs"]["zeta"] = {"snapshot-id": current_id, "type": "branch"}
            metadata["refs"]["alpha"] = {"snapshot-id": current_id, "type": "tag"}

        _, table_name = registered_metadata_table(
            "default.expire_snapshots_non_main_noop_source",
            "default.expire_snapshots_non_main_noop",
            1,
            add_non_main_refs,
        )

        with duckdb_runner() as test:
            test.statement_ok("CALL enable_logging('Iceberg')")
            test.statement_ok("CALL truncate_duckdb_logs()")
            test.statement_ok("BEGIN")
            test.statement_error(
                _expire_query(table_name, "retain_last => 10"),
                'found unsupported snapshot reference "alpha"',
            )
            # DuckDB aborts an explicit transaction after a statement error.
            # Rolling it back must not reveal a staged metadata mutation or POST.
            test.statement_ok("ROLLBACK")
            test.query("I", f"SELECT count(*) FROM iceberg_snapshots({table_name})", [(1,)])
            test.query(
                "I",
                """
                SELECT count(*)
                FROM duckdb_logs()
                WHERE type = 'Iceberg'
                  AND message LIKE 'POST % body=%'
                """,
                [(0,)],
            )

    @pytest.mark.parametrize(
        ("shape", "expected_error"),
        (
            ("main-is-tag", "Snapshot ref 'main' must be a branch"),
            ("main-current-mismatch", "does not match current-snapshot-id"),
            ("main-without-current", "requires current-snapshot-id"),
            ("current-unknown", "current-snapshot-id points to unknown snapshot"),
            ("main-max-age-zero", "Invalid max-snapshot-age-ms"),
            ("main-min-count-zero", "Invalid min-snapshots-to-keep"),
        ),
    )
    def test_invalid_explicit_main_metadata(
        self,
        shape,
        expected_error,
        seeded_table,
        duckdb_runner,
    ):
        sql_shape = shape.replace("-", "_")
        table, _ = seeded_table(f"default.expire_snapshots_{sql_shape}_source", 2)
        metadata = table.metadata.model_dump(mode="json", by_alias=True, exclude_none=True)

        def make_main_invalid(metadata):
            if shape == "main-is-tag":
                metadata["refs"]["main"]["type"] = "tag"
            elif shape == "main-current-mismatch":
                snapshots = sorted(metadata["snapshots"], key=lambda snapshot: snapshot["sequence-number"])
                metadata["refs"]["main"]["snapshot-id"] = snapshots[0]["snapshot-id"]
            elif shape == "main-without-current":
                metadata.pop("current-snapshot-id", None)
            elif shape == "current-unknown":
                unknown_id = max(snapshot["snapshot-id"] for snapshot in metadata["snapshots"]) + 1
                metadata["current-snapshot-id"] = unknown_id
                metadata["refs"]["main"]["snapshot-id"] = unknown_id
            elif shape == "main-max-age-zero":
                metadata["refs"]["main"]["max-snapshot-age-ms"] = 0
            else:
                metadata["refs"]["main"]["min-snapshots-to-keep"] = 0

        make_main_invalid(metadata)
        with _serve_table_metadata(table.metadata_location, metadata) as port:
            with duckdb_runner() as test:
                _attach_metadata_catalog(test, "invalid_datalake", port)
                test.statement_error(
                    _expire_query("invalid_datalake.default.invalid_main"),
                    expected_error,
                )

    def test_empty_refs_with_current_is_rejected_without_staging(self, seeded_table, duckdb_runner):
        table, _ = seeded_table("default.expire_snapshots_empty_refs_source", 2)
        metadata = table.metadata.model_dump(mode="json", by_alias=True, exclude_none=True)
        metadata["refs"] = {}

        with _serve_table_metadata(table.metadata_location, metadata) as port:
            with duckdb_runner() as test:
                test.statement_ok("CALL enable_logging('Iceberg')")
                test.statement_ok("CALL truncate_duckdb_logs()")
                _attach_metadata_catalog(test, "empty_refs_datalake", port)
                test.statement_error(
                    _expire_query("empty_refs_datalake.default.empty_refs"),
                    "Snapshot refs must contain 'main'",
                )
                test.query(
                    "I",
                    "SELECT count(*) FROM iceberg_snapshots(empty_refs_datalake.default.empty_refs)",
                    [(2,)],
                )
                test.query(
                    "I",
                    """
                    SELECT count(*)
                    FROM duckdb_logs()
                    WHERE type = 'Iceberg'
                      AND message LIKE 'POST % body=%'
                    """,
                    [(0,)],
                )

    def test_empty_refs_without_current_is_accepted(self, seeded_table, duckdb_runner):
        table, _ = seeded_table("default.expire_snapshots_empty_refs_no_current_source", 2)
        metadata = table.metadata.model_dump(mode="json", by_alias=True, exclude_none=True)
        metadata.pop("current-snapshot-id", None)
        metadata["refs"] = {}
        metadata["snapshot-log"] = []

        with _serve_table_metadata(table.metadata_location, metadata) as port:
            with duckdb_runner() as test:
                _attach_metadata_catalog(test, "empty_refs_no_current_datalake", port)
                _check_expiration(
                    test,
                    "empty_refs_no_current_datalake.default.empty_refs_no_current",
                    (0, 2),
                    "retain_last => 10",
                    "older_than => TIMESTAMP '1970-01-01'",
                )

    @JAVA_FIXTURE_ONLY
    def test_non_monotonic_main_retention_prefix(self, registered_metadata_table, duckdb_runner):
        def set_non_monotonic_timestamps(metadata):
            snapshots = sorted(metadata["snapshots"], key=lambda snapshot: snapshot["sequence-number"])
            for snapshot, timestamp_ms in zip(snapshots, (200, 100, 300)):
                snapshot["timestamp-ms"] = timestamp_ms

        _, table_name = registered_metadata_table(
            "default.expire_snapshots_prefix_source",
            "default.expire_snapshots_prefix",
            3,
            set_non_monotonic_timestamps,
        )

        with duckdb_runner() as test:
            test.statement_ok("BEGIN")
            _check_expiration(
                test,
                table_name,
                (2, 1),
                "retain_last => 1",
                "older_than => make_timestamp_ms(150)",
            )
            test.query("I", f"SELECT count(*) FROM iceberg_snapshots({table_name})", [(1,)])
            test.statement_ok("ROLLBACK")

    @JAVA_FIXTURE_ONLY
    def test_cycle_detected_after_retention_prefix_closes(self, registered_metadata_table, duckdb_runner):
        def create_cycle_after_expired_snapshot(metadata):
            snapshots = sorted(metadata["snapshots"], key=lambda snapshot: snapshot["sequence-number"])
            for snapshot, timestamp_ms in zip(snapshots, (200, 100, 300)):
                snapshot["timestamp-ms"] = timestamp_ms
            snapshots[0]["parent-snapshot-id"] = snapshots[1]["snapshot-id"]

        _, table_name = registered_metadata_table(
            "default.expire_snapshots_cycle_source",
            "default.expire_snapshots_cycle",
            3,
            create_cycle_after_expired_snapshot,
        )

        with duckdb_runner() as test:
            test.statement_error(
                _expire_query(
                    table_name,
                    "retain_last => 1",
                    "older_than => make_timestamp_ms(150)",
                ),
                "Cycle detected in snapshot ancestry",
            )
            test.query("I", f"SELECT count(*) FROM iceberg_snapshots({table_name})", [(3,)])

    @JAVA_FIXTURE_ONLY
    def test_legacy_implicit_main_commit_and_requirement(self, registered_metadata_table, duckdb_runner):
        def remove_refs(metadata):
            metadata.pop("refs", None)

        table, table_name = registered_metadata_table(
            "default.expire_snapshots_legacy_source",
            "default.expire_snapshots_legacy",
            3,
            remove_refs,
        )
        current_id = table.current_snapshot().snapshot_id

        with duckdb_runner() as test:
            test.statement_error(
                _expire_query(table_name, f"snapshot_ids => [{current_id}]"),
                "still referenced",
            )
            test.statement_ok("CALL enable_logging('Iceberg')")
            test.statement_ok("CALL truncate_duckdb_logs()")
            _check_expiration(
                test,
                table_name,
                (2, 1),
                "retain_last => 1",
                "older_than => TIMESTAMP '2999-01-01'",
            )
            test.query(
                "III",
                f"""
                WITH commit_log AS (
                    SELECT message
                    FROM duckdb_logs()
                    WHERE type = 'Iceberg'
                      AND message LIKE 'POST % body=%'
                    ORDER BY timestamp DESC
                    LIMIT 1
                )
                SELECT
                    list_count(regexp_extract_all(message, '"type":"assert-ref-snapshot-id"')),
                    list_count(regexp_extract_all(message, '"ref":"main"')),
                    CAST(contains(message, '"snapshot-id":{current_id}') AS INTEGER)
                FROM commit_log
                """,
                [(1, 1, 1)],
            )
            test.query("I", f"SELECT count(*) FROM iceberg_snapshots({table_name})", [(1,)])
            test.query("I", f"SELECT count(*) FROM {table_name} AT (VERSION => {current_id})", [(3,)])

        table.refresh()
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {current_id}

    @JAVA_FIXTURE_ONLY
    def test_explicit_id_removes_recent_unreferenced_snapshot(self, registered_metadata_table, duckdb_runner):
        def remove_main(metadata):
            metadata.pop("current-snapshot-id", None)
            metadata.pop("refs", None)
            metadata["snapshot-log"] = []

        table, table_name = registered_metadata_table(
            "default.expire_snapshots_no_current_explicit_source",
            "default.expire_snapshots_no_current_explicit",
            1,
            remove_main,
        )
        snapshot_id = table.metadata.snapshots[0].snapshot_id

        with duckdb_runner() as test:
            _check_expiration(
                test,
                table_name,
                (1, 0),
                f"snapshot_ids => [{snapshot_id}]",
                "retain_last => 10",
                "older_than => TIMESTAMP '1970-01-01'",
            )

        table.refresh()
        assert not table.metadata.snapshots

    @JAVA_FIXTURE_ONLY
    def test_no_current_snapshots_use_global_cutoff_and_null_main_requirement(
        self,
        registered_metadata_table,
        duckdb_runner,
    ):
        def remove_main(metadata):
            metadata.pop("current-snapshot-id", None)
            metadata.pop("refs", None)
            metadata["snapshot-log"] = []
            snapshots = sorted(metadata["snapshots"], key=lambda snapshot: snapshot["sequence-number"])
            snapshots[0]["timestamp-ms"] = 100
            snapshots[1]["timestamp-ms"] = 200

        table, table_name = registered_metadata_table(
            "default.expire_snapshots_no_current_source",
            "default.expire_snapshots_no_current",
            2,
            remove_main,
        )
        snapshots = _ordered_snapshots(table)
        old_id = snapshots[0].snapshot_id
        recent_id = snapshots[1].snapshot_id
        assert table.current_snapshot() is None

        with duckdb_runner() as test:
            test.statement_ok("CALL enable_logging('Iceberg')")
            test.statement_ok("CALL truncate_duckdb_logs()")
            _check_expiration(
                test,
                table_name,
                (1, 1),
                "retain_last => 10",
                "older_than => make_timestamp_ms(150)",
            )
            test.query(
                "IIIII",
                f"""
                WITH commit_log AS (
                    SELECT message
                    FROM duckdb_logs()
                    WHERE type = 'Iceberg'
                      AND message LIKE 'POST % body=%'
                    ORDER BY timestamp DESC
                    LIMIT 1
                )
                SELECT
                    list_count(regexp_extract_all(message, '"action":"remove-snapshots"')),
                    list_count(regexp_extract_all(message, '"type":"assert-ref-snapshot-id"')),
                    list_count(regexp_extract_all(message, '"ref":"main"')),
                    list_count(regexp_extract_all(message, '"snapshot-id":null')),
                    CAST(contains(message, '"snapshot-ids":[{old_id}]') AS INTEGER)
                FROM commit_log
                """,
                [(1, 1, 1, 1, 1)],
            )

        table.refresh()
        assert table.current_snapshot() is None
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {recent_id}

    def test_main_min_snapshot_count_retains_globally_old_ancestor(self, seeded_table, duckdb_runner):
        table, table_name = seeded_table("default.expire_snapshots_main_min_count", 3)
        snapshots = _ordered_snapshots(table)
        current_id = snapshots[-1].snapshot_id
        table.manage_snapshots().create_branch(current_id, "main", min_snapshots_to_keep=2).commit()

        with duckdb_runner() as test:
            _check_expiration(
                test,
                table_name,
                (1, 2),
                "older_than => TIMESTAMP '2999-01-01'",
                "retain_last => 1",
            )

        table.refresh()
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {
            snapshots[-2].snapshot_id,
            current_id,
        }

    def test_main_max_snapshot_age_overrides_global_cutoff(self, seeded_table, duckdb_runner):
        table, table_name = seeded_table("default.expire_snapshots_main_max_age", 3)
        snapshots = _ordered_snapshots(table)
        current_id = snapshots[-1].snapshot_id

        table.manage_snapshots().create_branch(
            current_id,
            "main",
            max_snapshot_age_ms=1,
            min_snapshots_to_keep=1,
        ).commit()
        sleep(0.01)

        with duckdb_runner() as test:
            _check_expiration(
                test,
                table_name,
                (2, 1),
                "older_than => TIMESTAMP '1970-01-01'",
                "retain_last => 3",
            )

        table.refresh()
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {current_id}

    @JAVA_FIXTURE_ONLY
    def test_retained_main_child_with_missing_parent(self, registered_metadata_table, duckdb_runner):
        def remove_middle_parent(metadata):
            snapshots = sorted(metadata["snapshots"], key=lambda snapshot: snapshot["sequence-number"])
            missing_parent_id = snapshots[1]["snapshot-id"]
            metadata["snapshots"] = [
                snapshot for snapshot in metadata["snapshots"] if snapshot["snapshot-id"] != missing_parent_id
            ]

        table, table_name = registered_metadata_table(
            "default.expire_snapshots_missing_parent_source",
            "default.expire_snapshots_missing_parent",
            3,
            remove_middle_parent,
        )
        current_id = table.current_snapshot().snapshot_id

        with duckdb_runner() as test:
            _check_expiration(
                test,
                table_name,
                (1, 1),
                "retain_last => 1",
                "older_than => TIMESTAMP '2999-01-01'",
            )

        table.refresh()
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {current_id}

    def test_side_effect_execution_boundary(self, seeded_table, duckdb_runner):
        table, table_name = seeded_table("default.expire_snapshots_execution_boundary", 3)
        snapshots = _ordered_snapshots(table)
        expiring_id = snapshots[0].snapshot_id
        expiration_query = _expire_query(
            table_name,
            f"snapshot_ids => [{expiring_id}]",
            "retain_last => 3",
        )

        with duckdb_runner() as test:
            # Binding and planning do not execute the source operator. A source
            # eliminated before it is pulled does not stage an update either.
            test.statement_ok(f"PREPARE expire_boundary AS {expiration_query}")
            test.statement_ok(f"EXPLAIN {expiration_query}")
            test.query("I", f"SELECT count(*) FROM ({expiration_query} LIMIT 0)", [(0,)])
            test.query("I", f"SELECT count(*) FROM ({expiration_query} WHERE false)", [(0,)])

        table.refresh()
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {
            snapshot.snapshot_id for snapshot in snapshots
        }

        with duckdb_runner() as test:
            test.query("I", f"SELECT count(*) FROM ({expiration_query})", [(1,)])

        table.refresh()
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {
            snapshots[1].snapshot_id,
            snapshots[2].snapshot_id,
        }

    def test_explain_analyze_executes_expiration(self, seeded_table, duckdb_runner):
        table, table_name = seeded_table("default.expire_snapshots_explain_analyze", 3)
        snapshots = _ordered_snapshots(table)
        expiring_id = snapshots[0].snapshot_id
        expiration_query = _expire_query(
            table_name,
            f"snapshot_ids => [{expiring_id}]",
            "retain_last => 3",
        )

        with duckdb_runner() as test:
            test.statement_ok(f"EXPLAIN ANALYZE {expiration_query}")

        table.refresh()
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {
            snapshots[1].snapshot_id,
            snapshots[2].snapshot_id,
        }

    def test_downstream_failure_rolls_back_staged_expiration(self, seeded_table, duckdb_runner):
        table, table_name = seeded_table("default.expire_snapshots_downstream_failure", 3)
        snapshots = _ordered_snapshots(table)
        expiring_id = snapshots[0].snapshot_id
        expiration_query = _expire_query(
            table_name,
            f"snapshot_ids => [{expiring_id}]",
            "retain_last => 3",
        )

        with duckdb_runner() as test:
            test.statement_ok("CALL enable_logging('Iceberg')")
            test.statement_ok("CALL truncate_duckdb_logs()")
            test.statement_ok("BEGIN")
            test.statement_error(
                f"""
                SELECT error('forced downstream failure after source row '
                             || CAST(deleted_snapshots AS VARCHAR))
                FROM ({expiration_query})
                """,
                "forced downstream failure after source row",
            )
            # Committing an aborted DuckDB transaction performs an implicit rollback.
            test.statement_ok("COMMIT")
            test.query("I", f"SELECT count(*) FROM iceberg_snapshots({table_name})", [(3,)])
            test.query(
                "I",
                """
                SELECT count(*)
                FROM duckdb_logs()
                WHERE type = 'Iceberg'
                  AND message LIKE 'POST % body=%'
                """,
                [(0,)],
            )

        table.refresh()
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {
            snapshot.snapshot_id for snapshot in snapshots
        }

    def test_standard_rest_requirement_surface(self, seeded_table, duckdb_runner):
        table, table_name = seeded_table("default.expire_snapshots_requirements", 5)
        snapshots = _ordered_snapshots(table)
        first_expiring_id = snapshots[0].snapshot_id
        second_expiring_id = snapshots[1].snapshot_id
        current_id = snapshots[-1].snapshot_id
        expected_snapshot_ids = ",".join(
            str(snapshot_id) for snapshot_id in sorted((first_expiring_id, second_expiring_id))
        )

        with duckdb_runner() as test:
            test.statement_ok("CALL enable_logging('Iceberg')")
            test.statement_ok("CALL truncate_duckdb_logs()")
            test.statement_ok("BEGIN")
            _check_expiration(
                test,
                table_name,
                (1, 4),
                f"snapshot_ids => [{first_expiring_id}]",
                "retain_last => 5",
            )
            _check_expiration(
                test,
                table_name,
                (1, 3),
                f"snapshot_ids => [{second_expiring_id}]",
                "retain_last => 5",
            )
            test.statement_ok("COMMIT")
            test.query(
                "IIIIIII",
                f"""
                WITH commit_log AS (
                    SELECT message
                    FROM duckdb_logs()
                    WHERE type = 'Iceberg'
                      AND message LIKE 'POST % body=%'
                    ORDER BY timestamp DESC
                    LIMIT 1
                )
                SELECT
                    list_count(regexp_extract_all(message, '"action":"remove-snapshots"')),
                    list_count(string_split(regexp_extract(message, '"snapshot-ids":\\[([^]]*)\\]', 1), ',')),
                    list_count(regexp_extract_all(message, '"type":"assert-ref-snapshot-id"')),
                    list_count(regexp_extract_all(message, '"ref":"main"')),
                    list_count(regexp_extract_all(message, '"type":"assert-table-uuid"')),
                    list_count(regexp_extract_all(message, '"type":"assert-')),
                    CAST(
                        contains(message, '"snapshot-id":{current_id}')
                        AND contains(message, '"snapshot-ids":[{expected_snapshot_ids}]')
                        AS INTEGER
                    )
                FROM commit_log
                """,
                [(1, 2, 1, 1, 1, 2, 1)],
            )

        table.refresh()
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {
            snapshots[2].snapshot_id,
            snapshots[3].snapshot_id,
            snapshots[4].snapshot_id,
        }

    def test_rollback_expires_abandoned_history(self, seeded_table, duckdb_runner):
        table, table_name = seeded_table("default.expire_snapshots_rollback", 3)
        snapshots = _ordered_snapshots(table)
        rollback_id = snapshots[1].snapshot_id

        with duckdb_runner() as test:
            test.statement_ok(f"CALL iceberg_rollback_to_snapshot('{table_name}', {rollback_id})")

        table.refresh()
        assert table.metadata.snapshot_log[-2].snapshot_id == snapshots[2].snapshot_id
        assert table.metadata.snapshot_log[-1].snapshot_id == rollback_id
        abandoned_timestamp_ms = table.metadata.snapshot_log[-2].timestamp_ms
        rollback_timestamp_ms = table.metadata.snapshot_log[-1].timestamp_ms

        with duckdb_runner() as test:
            test.query("I", _timestamp_time_travel_query(table_name, abandoned_timestamp_ms), [(3,)])
            test.statement_ok("BEGIN")
            _check_expiration(test, table_name, (2, 1), "retain_last => 1", "older_than => TIMESTAMP '2999-01-01'")
            # Truncating through the expired log entry prevents fallback to an older main state.
            test.query("I", _timestamp_time_travel_query(table_name, abandoned_timestamp_ms), [(0,)])
            test.query("I", _timestamp_time_travel_query(table_name, rollback_timestamp_ms), [(2,)])
            test.statement_ok("COMMIT")
            test.query("I", _timestamp_time_travel_query(table_name, abandoned_timestamp_ms), [(0,)])
            test.query("I", _timestamp_time_travel_query(table_name, rollback_timestamp_ms), [(2,)])
            test.query("II", f"SELECT count(*), sum(id) FROM {table_name}", [(2, 3)])

        table.refresh()
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {rollback_id}
        assert [entry.snapshot_id for entry in table.metadata.snapshot_log] == [rollback_id]

    def test_duplicate_snapshot_log_id_trims_through_last_occurrence(self, seeded_table, duckdb_runner):
        table, table_name = seeded_table(
            "default.expire_snapshots_duplicate_log",
            2,
            distinct_timestamps=True,
        )
        initial_snapshots = _ordered_snapshots(table)
        first_id = initial_snapshots[0].snapshot_id
        retained_id = initial_snapshots[1].snapshot_id

        with duckdb_runner() as test:
            test.statement_ok(f"CALL iceberg_rollback_to_snapshot('{table_name}', {first_id})")
        sleep(0.01)
        with duckdb_runner() as test:
            test.statement_ok(f"INSERT INTO {table_name} VALUES (3)")

        table.refresh()
        current_id = table.current_snapshot().snapshot_id
        log_ids = [entry.snapshot_id for entry in table.metadata.snapshot_log]
        assert log_ids[-4:] == [first_id, retained_id, first_id, current_id]
        retained_log_timestamp_ms = next(
            entry.timestamp_ms for entry in table.metadata.snapshot_log if entry.snapshot_id == retained_id
        )

        with duckdb_runner() as test:
            test.query("I", _timestamp_time_travel_query(table_name, retained_log_timestamp_ms), [(2,)])
            test.statement_ok("BEGIN")
            _check_expiration(
                test,
                table_name,
                (1, 2),
                f"snapshot_ids => [{first_id}]",
                "retain_last => 10",
            )
            # The last S1 occurrence is after S2, so trimming only through the
            # first S1 would incorrectly leave a timestamp lookup across the gap.
            test.query("I", _timestamp_time_travel_query(table_name, retained_log_timestamp_ms), [(0,)])
            test.query(
                "II",
                f"SELECT count(*), sum(id) FROM {table_name} AT (VERSION => {retained_id})",
                [(2, 3)],
            )
            # The local mirror is already trimmed. A repeated call is a no-op,
            # but the first call's update remains pending and is committed.
            _check_expiration(
                test,
                table_name,
                (0, 2),
                "retain_last => 10",
            )
            test.statement_ok("COMMIT")
            test.query("I", _timestamp_time_travel_query(table_name, retained_log_timestamp_ms), [(0,)])
            test.query(
                "II",
                f"SELECT count(*), sum(id) FROM {table_name} AT (VERSION => {retained_id})",
                [(2, 3)],
            )

        table.refresh()
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {retained_id, current_id}
        assert [entry.snapshot_id for entry in table.metadata.snapshot_log] == [current_id]

    def test_snapshot_log_gap_is_a_history_boundary(self, seeded_table, duckdb_runner):
        table, table_name = seeded_table("default.expire_snapshots_log_gap", 3, distinct_timestamps=True)
        snapshots = _ordered_snapshots(table)
        first_id = snapshots[0].snapshot_id
        missing_id = snapshots[1].snapshot_id
        current_id = snapshots[2].snapshot_id
        missing_timestamp_ms = snapshots[1].timestamp_ms
        assert missing_timestamp_ms < snapshots[2].timestamp_ms

        with duckdb_runner() as test:
            test.statement_ok("BEGIN")
            _check_expiration(
                test,
                table_name,
                (1, 2),
                f"snapshot_ids => [{missing_id}]",
                "retain_last => 3",
            )
            # S2 is the newest log entry at this timestamp. Its absence is a
            # boundary; timestamp lookup must not cross the gap and return S1.
            test.query("I", _timestamp_time_travel_query(table_name, missing_timestamp_ms), [(0,)])
            test.query("II", f"SELECT count(*), sum(id) FROM {table_name} AT (VERSION => {first_id})", [(1, 1)])
            test.statement_ok("COMMIT")
            test.query("I", _timestamp_time_travel_query(table_name, missing_timestamp_ms), [(0,)])
            test.query("II", f"SELECT count(*), sum(id) FROM {table_name} AT (VERSION => {first_id})", [(1, 1)])

        table.refresh()
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {first_id, current_id}
        assert missing_id not in {snapshot.snapshot_id for snapshot in table.metadata.snapshots}

    def test_concurrent_expiration_of_disjoint_snapshots(self, seeded_table, duckdb_runner):
        table, table_name = seeded_table("default.expire_snapshots_concurrent", 4)
        snapshots = _ordered_snapshots(table)

        with duckdb_runner() as test:
            for connection, snapshot in zip(("con1", "con2"), snapshots[:2]):
                test.statement_ok("BEGIN", connection=connection)
                _check_expiration(
                    test,
                    table_name,
                    (1, 3),
                    f"snapshot_ids => [{snapshot.snapshot_id}]",
                    "retain_last => 4",
                    connection=connection,
                )

            test.statement_ok("COMMIT", connection="con1")
            test.statement_ok("COMMIT", connection="con2")

        table.refresh()
        assert {snapshot.snapshot_id for snapshot in table.metadata.snapshots} == {
            snapshots[2].snapshot_id,
            snapshots[3].snapshot_id,
        }
