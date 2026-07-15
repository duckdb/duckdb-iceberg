#!/usr/bin/env python3
"""
Minimal mock REST catalog server for testing SigV4 HTTP scheme handling.
Accepts any request with SigV4 Authorization header and returns valid Iceberg REST responses.

It also serves an AWS STS `AssumeRoleWithWebIdentity` endpoint that issues
short-lived, ROTATING credentials on each call, so tests can exercise the
credential auto-refresh path. The access key id encodes a monotonically
increasing counter (e.g. `MOCKKEY0`, `MOCKKEY1`, ...) and a `/refresh_count`
endpoint reports how many times STS has been called, letting a test assert the
refresh fired the expected number of times.

Usage:
    python3 sigv4_mock_server.py [port]
    Default port: 19130

To point the DuckDB aws extension at this mock STS endpoint, set:
    AWS_ENDPOINT_URL_STS=http://127.0.0.1:19130
    AWS_ROLE_ARN=arn:aws:iam::123456789012:role/mock-role
    AWS_WEB_IDENTITY_TOKEN_FILE=<path to any non-empty file>
    AWS_REGION=us-east-1
"""

import json
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

# Guards the shared counter against concurrent STS requests.
_lock = threading.Lock()
_sts_call_count = 0


def _next_credentials():
    """Return a fresh, rotating set of temporary credentials."""
    global _sts_call_count
    with _lock:
        n = _sts_call_count
        _sts_call_count += 1
    # Access key encodes the counter so callers can detect rotation.
    return f"MOCKKEY{n}", f"mock-secret-{n}", f"mock-session-token-{n}"


def _assume_role_web_identity_response():
    access_key, secret_key, session_token = _next_credentials()
    # Minimal AssumeRoleWithWebIdentity XML response with a far-future expiry.
    return f"""<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>{access_key}</AccessKeyId>
      <SecretAccessKey>{secret_key}</SecretAccessKey>
      <SessionToken>{session_token}</SessionToken>
      <Expiration>2999-01-01T00:00:00Z</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <Arn>arn:aws:sts::123456789012:assumed-role/mock-role/mock</Arn>
      <AssumedRoleId>MOCKROLEID:mock</AssumedRoleId>
    </AssumedRoleUser>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>"""


def _assume_role_response():
    access_key, secret_key, session_token = _next_credentials()
    # Minimal AssumeRole XML response with a far-future expiry. The `sts` chain
    # (regular Aws::STS::STSClient) honors AWS_ENDPOINT_URL_STS and calls the
    # plain AssumeRole action, unlike the CRT-based web_identity provider.
    return f"""<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>{access_key}</AccessKeyId>
      <SecretAccessKey>{secret_key}</SecretAccessKey>
      <SessionToken>{session_token}</SessionToken>
      <Expiration>2999-01-01T00:00:00Z</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <Arn>arn:aws:sts::123456789012:assumed-role/mock-role/mock</Arn>
      <AssumedRoleId>MOCKROLEID:mock</AssumedRoleId>
    </AssumedRoleUser>
  </AssumeRoleResult>
</AssumeRoleResponse>"""


class MockCatalogHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/refresh_count"):
            with _lock:
                count = _sts_call_count
            self._respond(200, {"sts_call_count": count})
        elif self.path.startswith("/v1/config"):
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
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8", "ignore") if length else ""
        # STS actions arrive as form-encoded bodies containing Action=AssumeRole...
        if "AssumeRoleWithWebIdentity" in body:
            self._respond(200, _assume_role_web_identity_response(), "text/xml")
        elif "AssumeRole" in body or self.path.startswith("/sts"):
            self._respond(200, _assume_role_response(), "text/xml")
        else:
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
