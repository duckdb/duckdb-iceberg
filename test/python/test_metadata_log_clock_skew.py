import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.parametrize(
    "allowance,latest_offset,history,expected",
    [
        (None, -1, [], "latest"),
        (None, 0, [], "latest"),
        (None, 1, [(0, "old")], "old"),
        (None, 1, [], None),
        (100, 99, [], "latest"),
        (100, 100, [], "latest"),
        (100, 101, [(-1, "old")], "old"),
        (100, 102, [(-1, "old"), (100, "boundary"), (101, "too_new")], "boundary"),
        (100, 101, [], None),
        (100, 102, [(101, "too_new")], None),
        (0, 0, [], "latest"),
        (0, 1, [(0, "old")], "old"),
        (250, 250, [], "latest"),
        (250, 251, [(250, "boundary")], "boundary"),
        (9223372036854775807, 1000, [], "latest"),
    ],
)
def test_metadata_log_clock_skew(
    tmp_path, unittest_binary, print_unittest_stdin, allowance, latest_offset, history, expected
):
    # Use DuckDB's transaction timestamp, not the server's wall clock, so these
    # cases exercise exact millisecond boundaries without sleeps or timing races.
    start_file = tmp_path / "transaction_start.csv"

    def metadata(timestamp, probe):
        return {
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000001",
            "location": str(tmp_path),
            "last-updated-ms": timestamp,
            "last-column-id": 1,
            "last-sequence-number": 0,
            "current-schema-id": 0,
            "schemas": [
                {"schema-id": 0, "type": "struct", "fields": [{"id": 1, "name": "a", "required": False, "type": "int"}]}
            ],
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "properties": {"clock-skew.probe": probe},
        }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith("/v1/config"):
                response = {"defaults": {}, "overrides": {}}
            elif self.path == "/v1/namespaces":
                response = {"namespaces": [["default"]]}
            elif self.path == "/v1/namespaces/default":
                response = {"namespace": ["default"], "properties": {}}
            elif self.path == "/v1/namespaces/default/tables":
                response = {"identifiers": [{"namespace": ["default"], "name": "clock_skew"}]}
            elif self.path == "/v1/namespaces/default/tables/clock_skew":
                start = int(start_file.read_text().strip())
                latest = metadata(start + latest_offset, "latest")
                latest["metadata-log"] = []
                for offset, probe in history:
                    path = tmp_path / f"{probe}.metadata.json"
                    path.write_text(json.dumps(metadata(start + offset, probe)))
                    latest["metadata-log"].append({"timestamp-ms": start + offset, "metadata-file": str(path)})
                response = {"metadata-location": str(tmp_path / "latest.metadata.json"), "metadata": latest}
            else:
                self.send_error(404)
                return
            body = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "statically_loaded_extensions": ["core_functions", "parquet", "avro", "iceberg", "httpfs"],
                "test_env": [{"env_name": "CATALOG_TEST_CONFIG_SETUP", "env_value": "clock_skew"}],
            }
        )
    )
    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with DuckDBUnittestRunner(unittest_binary, test_config=config, print_stdin=print_unittest_stdin) as test:
                if allowance is not None:
                    test.statement_ok(f"SET iceberg_metadata_log_clock_skew_ms = {allowance}")
                test.statement_ok(
                    f"ATTACH '' AS skew_catalog (TYPE iceberg, URI 'http://127.0.0.1:{server.server_port}', "
                    "AUTHORIZATION_TYPE 'none')"
                )
                with test.with_transaction(commit=False):
                    test.statement_ok(
                        f"COPY (SELECT epoch_us(current_timestamp) // 1000) TO '{start_file}' (HEADER false)"
                    )
                    query = (
                        "SELECT value FROM iceberg_table_properties(skew_catalog.default.clock_skew) "
                        "WHERE key = 'clock-skew.probe'"
                    )
                    if expected is None:
                        error = "metadata-log has no entry" if history else "does not contain a metadata-log"
                        test.statement_error(query, error)
                    else:
                        test.query("I", query, [(expected,)])
        finally:
            server.shutdown()
            thread.join()
