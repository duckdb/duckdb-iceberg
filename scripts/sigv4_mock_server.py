#!/usr/bin/env python3
"""
Minimal mock REST catalog server for testing SigV4 HTTP scheme handling.
Accepts any request with SigV4 Authorization header and returns valid Iceberg REST responses.

Usage:
    python3 sigv4_mock_server.py [port]
    Default port: 19130
"""

import json
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler


class MockCatalogHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Check for SigV4 Authorization header
        auth = self.headers.get("Authorization", "")
        has_sigv4 = auth.startswith("AWS4-HMAC-SHA256")

        if self.path.startswith("/v1/config"):
            self._respond(200, {"defaults": {}, "overrides": {}})
        elif "/namespaces" in self.path:
            self._respond(200, {"namespaces": []})
        else:
            self._respond(200, {})

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

    def do_POST(self):
        self._respond(200, {})

    def _respond(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
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
