import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.parametrize("operation", ["load", "create", "response"])
@pytest.mark.parametrize(
    "restrictions,rejected",
    [
        pytest.param(None, False, id="absent"),
        pytest.param({}, False, id="empty"),
        pytest.param({"required-column-projections": []}, False, id="empty-projections"),
        pytest.param({"required-row-filter": True}, True, id="true-filter"),
        pytest.param({"required-row-filter": False}, True, id="false-filter"),
        pytest.param({"required-row-filter": {"type": "eq", "term": "a", "value": 1}}, True, id="comparison-filter"),
        pytest.param({"required-row-filter": {"type": "unknown-predicate"}}, True, id="unknown-filter"),
        pytest.param(
            {"required-column-projections": [{"field-id": 1, "action": "replace-with-null"}]},
            True,
            id="projection",
        ),
        pytest.param(
            {"required-column-projections": [{"field-id": 1, "action": "unknown-action"}]},
            True,
            id="unknown-projection",
        ),
    ],
)
def test_read_restrictions(tmp_path, unittest_binary, print_unittest_stdin, operation, restrictions, rejected):
    table_loads = []
    metadata = {
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000001",
        "location": str(tmp_path),
        "last-updated-ms": 0,
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
    }

    class Handler(BaseHTTPRequestHandler):
        def send_json(self, response, status=200):
            body = json.dumps(response).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def table_response(self, restricted=True):
            response = {"metadata": metadata}
            if restricted and restrictions is not None:
                response["read-restrictions"] = restrictions
            self.send_json(response)

        def do_GET(self):
            if self.path.startswith("/v1/config"):
                self.send_json({"defaults": {}, "overrides": {}})
            elif self.path == "/v1/namespaces":
                self.send_json({"namespaces": [["default"]]})
            elif self.path == "/v1/namespaces/default":
                self.send_json({"namespace": ["default"], "properties": {}})
            elif self.path == "/v1/namespaces/default/tables":
                self.send_json({"identifiers": [{"namespace": ["default"], "name": "restricted"}]})
            elif self.path == "/v1/namespaces/default/tables/restricted":
                table_loads.append(self.path)
                # The metadata function must also check its fresh response after
                # binding against an initially unrestricted table.
                self.table_response(restricted=operation != "response" or len(table_loads) > 1)
            elif self.path == "/v1/namespaces/default/tables/created":
                self.send_json(
                    {"error": {"message": "Table does not exist", "type": "NoSuchTableException", "code": 404}},
                    status=404,
                )
            else:
                self.send_error(404)

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            if self.path == "/v1/namespaces/default/tables":
                self.table_response()
            else:
                self.send_error(404)

        def log_message(self, *args):
            pass

    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "statically_loaded_extensions": ["core_functions", "parquet", "avro", "iceberg", "httpfs"],
                "test_env": [{"env_name": "CATALOG_TEST_CONFIG_SETUP", "env_value": "read_restrictions"}],
            }
        )
    )
    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with DuckDBUnittestRunner(unittest_binary, test_config=config, print_stdin=print_unittest_stdin) as test:
                test.statement_ok(
                    f"ATTACH '' AS restrictions_catalog (TYPE iceberg, URI 'http://127.0.0.1:{server.server_port}', "
                    "AUTHORIZATION_TYPE 'none')"
                )
                with test.with_transaction(commit=False):
                    if operation == "create":
                        query = "CREATE TABLE restrictions_catalog.default.created(a INTEGER)"
                    elif operation == "response":
                        query = (
                            "SELECT count(*) FROM "
                            "iceberg_load_table_response('restrictions_catalog.default.restricted')"
                        )
                    else:
                        query = "SELECT count(*) FROM restrictions_catalog.default.restricted"
                    if rejected:
                        test.statement_error(
                            query, "Not implemented Error: Iceberg read-restrictions are not supported"
                        )
                    elif operation == "create":
                        test.statement_ok(query)
                    else:
                        test.query("I", query, [(1 if operation == "response" else 0,)])
        finally:
            server.shutdown()
            thread.join()
