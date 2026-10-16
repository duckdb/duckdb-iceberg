#!/usr/bin/env python3
"""
Minimal mock REST catalog server for testing SigV4 HTTP scheme handling.
Accepts any request with SigV4 Authorization header and returns valid Iceberg REST responses.

It also records the access key id that signed the most recent request (parsed out
of the SigV4 `Credential=<key id>/...` field) and reports it at
`/last_signing_key`. That lets a test observe which credential the catalog is
signing with, e.g. to assert that a refreshed secret is actually picked up.

Usage:
    python3 sigv4_mock_server.py [port]
    Default port: 19130
"""

import json
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, uuid5

# Guards the recorded key id against concurrent requests.
_lock = threading.Lock()
_last_key_id = None


# Empty tables exercise credential registration without contacting object storage.
_scope_tables = {
    "single_prefix": ("s3://scope-bucket/table/_external_metadata", ["s3://scope-bucket/table/"]),
    "single_nested": ("s3://scope-bucket/table/", ["s3://scope-bucket/table/metadata/"]),
    "nested": ("s3://scope-bucket/table/", ["s3://scope-bucket/table/metadata/", "s3://scope-bucket/table/"]),
    "alias": ("s3a://scope-bucket/table/", ["s3://scope-bucket/table/metadata/"]),
    "different_bucket": ("s3://scope-bucket/table/", ["s3://other-bucket/table/"]),
    "slash": ("s3://scope-bucket/table/", ["/"]),
    "bare_scheme": ("s3://scope-bucket/table/", ["s3"]),
    "mixed_types": ("s3://scope-bucket/table/", ["gs://scope-bucket/table/", "s3://scope-bucket/table/"]),
}


def _scope_response(path):
    if path == "/v1/config":
        return {"defaults": {}, "overrides": {}}
    if path == "/v1/namespaces":
        return {"namespaces": [["default"]]}
    if path == "/v1/namespaces/default":
        return {"namespace": ["default"], "properties": {}}
    if path == "/v1/namespaces/default/tables":
        return {"identifiers": [{"namespace": ["default"], "name": name} for name in _scope_tables]}
    table_name = path.removeprefix("/v1/namespaces/default/tables/")
    if table_name not in _scope_tables:
        return None
    location, prefixes = _scope_tables[table_name]
    return {
        "metadata-location": location.rstrip("/") + "/metadata/v1.metadata.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": str(uuid5(NAMESPACE_URL, table_name)),
            "location": location,
            "last-sequence-number": 0,
            "last-updated-ms": 1,
            "last-column-id": 1,
            "schemas": [
                {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [{"id": 1, "name": "id", "required": False, "type": "long"}],
                }
            ],
            "current-schema-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "last-partition-id": 999,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "properties": {},
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
        },
        "storage-credentials": [
            {
                "prefix": prefix,
                "config": {
                    "s3.access-key-id": f"scope_key_{index}",
                    "s3.secret-access-key": "scope_secret",
                    "s3.region": "us-east-1",
                },
            }
            for index, prefix in enumerate(prefixes)
        ],
    }


def _record_signing_key(headers):
    """Remember the access key id from a SigV4 Authorization header, if present.

    The header looks like:
        AWS4-HMAC-SHA256 Credential=<key id>/<date>/<region>/<service>/aws4_request, ...
    """
    global _last_key_id
    auth = headers.get("Authorization")
    if not auth or "Credential=" not in auth:
        return
    credential = auth.split("Credential=", 1)[1]
    key_id = credential.split("/", 1)[0].strip().rstrip(",")
    if key_id:
        with _lock:
            _last_key_id = key_id


class MockCatalogHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Answer before recording: this endpoint is unsigned, so it must not
        # clobber the key id it is being asked about.
        if self.path.startswith("/last_signing_key"):
            with _lock:
                key_id = _last_key_id
            self._respond(200, {"key_id": key_id})
            return
        _record_signing_key(self.headers)
        path = urlsplit(self.path).path
        if path.startswith("/credential-scopes/"):
            response = _scope_response(path.removeprefix("/credential-scopes"))
            if response is None:
                self._respond(
                    404, {"error": {"message": "Unknown scope fixture", "type": "NoSuchTableException", "code": 404}}
                )
            else:
                self._respond(200, response)
            return
        if self.path.startswith("/v1/config"):
            self._respond(200, {"defaults": {}, "overrides": {}})
        elif "/namespaces" in self.path:
            self._respond(200, {"namespaces": []})
        else:
            self._respond(200, {})

    def do_HEAD(self):
        _record_signing_key(self.headers)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

    def do_POST(self):
        _record_signing_key(self.headers)
        length = int(self.headers.get("Content-Length", 0))
        if length:
            self.rfile.read(length)
        # Iceberg REST catalog POST (e.g. table load) — return empty JSON.
        self._respond(200, {})

    def _respond(self, code, body, content_type="application/json"):
        data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format, *args):
        # Suppress default logging
        pass


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 19130
    server = HTTPServer(("127.0.0.1", port), MockCatalogHandler)
    print(f"Mock SigV4 catalog server listening on http://127.0.0.1:{port}")
    sys.stdout.flush()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()


if __name__ == "__main__":
    main()
