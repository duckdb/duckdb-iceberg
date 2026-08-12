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

# Guards the recorded key id against concurrent requests.
_lock = threading.Lock()
_last_key_id = None


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
