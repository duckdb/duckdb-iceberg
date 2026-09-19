#!/usr/bin/env python3
"""
Mock Iceberg REST catalog that mimics AWS Glue's metadata reconstruction behavior.

AWS Glue's Iceberg REST endpoint does not necessarily return the contents of the
table's metadata file; it can reconstruct the table metadata from its own catalog
state (see duckdb-iceberg#1359). For tables registered through Hive-style Glue
APIs (e.g. Spark's GlueCatalog), the reconstructed schema can carry different
nested field ids and lose properties such as 'schema.name-mapping.default'.

This server serves a LoadTable response whose embedded `metadata` has been
"reconstructed" in that fashion, while `metadata-location` points at the
authoritative metadata file on disk (served as an absolute path).

Usage:
    python3 reconstructed_metadata_mock_server.py [port] [metadata_json_path]
    Default port: 19131
    Default metadata: data/persistent/reconstructed_metadata_response table,
                      resolved relative to the current working directory.
"""

import copy
import json
import os
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

DEFAULT_METADATA = (
    "data/persistent/reconstructed_metadata_response/warehouse/db/t/metadata/"
    "00001-cf9a460b-ae4d-411c-84bb-cc342e6ae3f9.metadata.json"
)

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 19131
METADATA_PATH = os.path.abspath(sys.argv[2] if len(sys.argv) > 2 else DEFAULT_METADATA)

with open(METADATA_PATH) as f:
    REAL_METADATA = json.load(f)


def reconstruct_like_glue(metadata):
    """Simulate Glue's server-side metadata reconstruction: reassign nested field
    ids (Glue's Hive-style column representation does not carry them) and drop
    properties such as 'schema.name-mapping.default'."""
    m = copy.deepcopy(metadata)
    m["properties"] = {k: v for k, v in m.get("properties", {}).items() if "name-mapping" not in k}
    next_fake_id = 1000

    def remap(fields):
        nonlocal next_fake_id
        for field in fields:
            ftype = field["type"]
            if isinstance(ftype, dict) and ftype.get("type") == "struct":
                for child in ftype["fields"]:
                    child["id"] = next_fake_id
                    next_fake_id += 1
                    remap(child["type"]["fields"] if isinstance(child["type"], dict) and child["type"].get("type") == "struct" else [])

    for schema in m.get("schemas", []):
        remap(schema["fields"])
    return m


RECONSTRUCTED = reconstruct_like_glue(REAL_METADATA)


class ReconstructedMetadataHandler(BaseHTTPRequestHandler):
    def _respond(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]
        parts = [p for p in path.split("/") if p]
        if path.startswith("/v1/config"):
            self._respond(200, {"defaults": {}, "overrides": {}})
            return
        if "namespaces" not in parts:
            self._respond(404, {"error": {"message": "not found", "type": "NotFound", "code": 404}})
            return
        tail = parts[parts.index("namespaces") + 1 :]
        if not tail:
            self._respond(200, {"namespaces": [["db"]]})
        elif len(tail) == 1:
            self._respond(200, {"namespace": ["db"], "properties": {}})
        elif tail == ["db", "tables"]:
            self._respond(200, {"identifiers": [{"namespace": ["db"], "name": "t"}]})
        elif tail == ["db", "tables", "t"]:
            self._respond(200, {"metadata-location": METADATA_PATH, "metadata": RECONSTRUCTED})
        else:
            self._respond(404, {"error": {"message": "not found", "type": "NotFound", "code": 404}})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        if length:
            self.rfile.read(length)
        self._respond(200, {})

    def log_message(self, fmt, *args):
        sys.stderr.write("reconstructed-metadata-mock %s\n" % (fmt % args))


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", PORT), ReconstructedMetadataHandler).serve_forever()
