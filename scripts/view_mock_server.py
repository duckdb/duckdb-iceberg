"""Deterministic Iceberg view protocol fixture; run with uv run scripts/view_mock_server.py."""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

VIEWS = {
    "preferred": [("spark", "select 99"), ("duckdb", "select 42")],
    "compatible": [("spark", "select 17")],
    "foreign": [("spark", "select `spark column` from `spark table`")],
    "foreign'quote": [("spark", "select `spark column` from `spark table`")],
    "no_sql": [],
    "not_select": [("duckdb", "drop table source")],
    "many_statements": [("duckdb", "select 1; select 2")],
    "namespace": [("duckdb", "select value from source")],
    "qualified": [("duckdb", "select value from other.source")],
    "explicit": [("duckdb", "select value from explicit_catalog.other.source")],
    "cte": [("duckdb", "with source as (select 61 as value) select value from source")],
    "subquery": [("duckdb", "select (select value from source) as value")],
    "scopes": [
        (
            "duckdb",
            "select value from source union all select value from (with source as (select 61 as value) select * from source)",
        )
    ],
    "recursive": [
        ("duckdb", "with recursive seq(n) as (select 1 union all select n+1 from seq where n<3) select max(n) from seq")
    ],
    "shadow": [("duckdb", "with source as (select value+1 as value from source) select value from source")],
}
ENDPOINTS = [
    "GET /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "GET /v1/{prefix}/namespaces/{namespace}/views",
    "GET /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}",
]


class ViewCatalogHandler(BaseHTTPRequestHandler):
    deleted = set()
    mutations = 0

    def respond(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def error(self, status):
        self.respond(
            status, {"error": {"message": f"view fixture error {status}", "type": "FixtureError", "code": status}}
        )

    def do_GET(self):
        url = urlsplit(self.path)
        path = unquote(url.path)
        if path == "/mutations":
            self.respond(200, {"mutations": type(self).mutations})
            return
        if path == "/v1/config":
            self.deleted.clear()
            type(self).mutations = 0
            no_views = parse_qs(url.query).get("warehouse") == ["no_views"]
            endpoints = [item for item in ENDPOINTS if not no_views or "/views" not in item]
            self.respond(200, {"defaults": {}, "overrides": {}, "endpoints": endpoints})
        elif path == "/v1/namespaces":
            self.respond(200, {"namespaces": [["default"]]})
        elif path == "/v1/namespaces/default":
            self.respond(200, {"namespace": ["default"], "properties": {}})
        elif path.endswith("/tables"):
            self.respond(200, {"identifiers": []})
        elif "/tables/" in path:
            self.error(404)
        elif path.endswith("/views"):
            # Two pages ensure discovery follows the continuation token.
            names = list(VIEWS)[2:] if parse_qs(url.query).get("pageToken") else ["preferred", "compatible"]
            body = {
                "identifiers": [{"namespace": ["default"], "name": name} for name in names if name not in self.deleted]
            }
            if not parse_qs(url.query).get("pageToken"):
                body["next-page-token"] = "second"
            self.respond(200, body)
        elif "/views/" in path:
            name = path.rsplit("/", 1)[1]
            if name.startswith("error_"):
                self.error(int(name.removeprefix("error_")))
            elif name not in VIEWS or name in self.deleted:
                self.error(404)
            else:
                self.respond(
                    200,
                    {
                        "metadata-location": f"s3://fixture/{name}.json",
                        "metadata": {
                            "view-uuid": "8c368527-e2a0-470b-8ab0-06a94e606f56",
                            "format-version": 1,
                            "location": f"s3://fixture/{name}",
                            "current-version-id": 1,
                            "schemas": [
                                {
                                    "schema-id": 0,
                                    "type": "struct",
                                    "fields": [{"id": 1, "name": "value", "required": False, "type": "int"}],
                                }
                            ],
                            "versions": [
                                {
                                    "version-id": 1,
                                    "timestamp-ms": 1700000000000,
                                    "schema-id": 0,
                                    "summary": {},
                                    "default-namespace": ["source_ns"],
                                    **(
                                        {"default-catalog": "source_catalog"}
                                        if name
                                        in {"namespace", "qualified", "explicit", "subquery", "scopes", "shadow"}
                                        else {}
                                    ),
                                    "representations": [
                                        {"type": "sql", "dialect": dialect, "sql": sql} for dialect, sql in VIEWS[name]
                                    ],
                                }
                            ],
                            "version-log": [{"timestamp-ms": 1700000000000, "version-id": 1}],
                        },
                    },
                )
        else:
            self.error(404)

    def do_DELETE(self):
        type(self).mutations += 1
        name = unquote(urlsplit(self.path).path).rsplit("/", 1)[1]
        if name not in VIEWS or name in self.deleted:
            self.error(404)
            return
        self.deleted.add(name)
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", 19134), ViewCatalogHandler).serve_forever()
