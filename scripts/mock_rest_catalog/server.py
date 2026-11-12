"""Loopback HTTP adapter and bounded request journal for the test catalog."""

import json
import secrets
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

from .model import Catalog, CatalogError, invalid, unsupported


ENDPOINTS = [
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "DELETE /v1/{prefix}/namespaces/{namespace}",
    "POST /v1/{prefix}/namespaces/{namespace}/properties",
    "POST /v1/{prefix}/tables/rename",
    "POST /v1/{prefix}/transactions/commit",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
]


class Handler(BaseHTTPRequestHandler):
    def route(self, body):
        catalog = self.server.catalog
        url = urlsplit(self.path)
        parts = [unquote(part) for part in url.path.split("/")[1:]]
        query = parse_qs(url.query)
        method = self.command
        if parts and parts[0] == "__mock" and self.server.control_token:
            if not secrets.compare_digest(self.headers.get("X-Mock-Token", ""), self.server.control_token):
                raise CatalogError(403, "ForbiddenException", "Invalid mock control token")
            if parts == ["__mock", "status"] and method == "GET":
                return 200, {"unsupported": len(self.server.unsupported), "errors": len(self.server.errors)}
            if parts == ["__mock", "stop"] and method == "POST":
                self.server.stop_requested.set()
                return 200, {}
        if parts == ["v1", "config"] and method == "GET":
            return 200, {"defaults": {}, "overrides": {}, "endpoints": ENDPOINTS}
        if parts == ["v1", "tables", "rename"] and method == "POST":
            catalog.rename(body)
            return 204, None
        if parts == ["v1", "transactions", "commit"] and method == "POST":
            catalog.commit_transaction(body)
            return 204, None
        if parts[:2] != ["v1", "namespaces"]:
            unsupported(f"Unknown route: {method} {url.path}")
        if len(parts) == 2:
            if method == "GET":
                parent = tuple(query["parent"][0].split("\x1f")) if "parent" in query else ()
                return 200, {
                    "namespaces": [
                        list(ns)
                        for ns in sorted(catalog.namespaces)
                        if ns[: len(parent)] == parent and len(ns) == len(parent) + 1
                    ]
                }
            if method == "POST":
                return 200, catalog.create_namespace(body)
        if len(parts) >= 3:
            namespace = tuple(parts[2].split("\x1f"))
            if len(parts) == 4 and parts[3] == "properties" and method == "POST":
                return 200, catalog.update_namespace_properties(namespace, body)
            if len(parts) == 3:
                if method == "GET":
                    return 200, catalog.namespace(namespace)
                if method == "DELETE":
                    catalog.drop_namespace(namespace)
                    return 204, None
            if len(parts) == 4 and parts[3] == "tables":
                catalog.namespace(namespace)
                if method == "GET":
                    return 200, {
                        "identifiers": [
                            {"namespace": list(ns), "name": name}
                            for ns, name in sorted(catalog.tables)
                            if ns == namespace
                        ]
                    }
                if method == "POST":
                    return 200, catalog.create(namespace, body)
            if len(parts) == 5 and parts[3] == "tables":
                key = namespace, parts[4]
                if method == "GET":
                    return 200, catalog.load(key)
                if method == "POST":
                    return 200, catalog.commit(key, body)
                if method == "DELETE":
                    if query.get("purgeRequested", ["false"])[0].lower() != "false":
                        unsupported("Purge is not implemented; normal drop only unregisters")
                    catalog.load(key)
                    del catalog.tables[key]
                    return 204, None
        unsupported(f"Unknown route: {method} {url.path}")

    def handle_request(self):
        body = None
        with self.server.catalog.lock:
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 0 or length > 16 * 1024 * 1024:
                    invalid("Request body exceeds 16 MiB")
                if length:
                    body = json.loads(self.rfile.read(length))
                    if not isinstance(body, dict):
                        invalid("Request body must be a JSON object")
                status, response = self.route(body)
            except CatalogError as error:
                status = error.code
                response = {"error": {"message": str(error), "type": error.kind, "code": status}}
                if status == 501:
                    self.server.unsupported.append(str(error))
            except (ValueError, KeyError, TypeError) as error:
                status = 400
                response = {"error": {"message": str(error), "type": "ValidationException", "code": status}}
            except Exception:
                status = 500
                response = {"error": {"message": "Mock server exception", "type": "ServerError", "code": status}}
                self.server.errors.append(traceback.format_exc())
            event = {"method": self.command, "path": self.path, "status": status}
            for key, value in (("request", body), ("response", response)):
                encoded = json.dumps(value)
                event[key] = value if len(encoded) <= 16384 else encoded[:16384] + "... [truncated]"
            with self.server.journal.open("a") as journal:
                journal.write(json.dumps(event) + "\n")
        data = json.dumps(response).encode() if response is not None else b""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        try:
            if self.command != "HEAD":
                self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    do_GET = do_POST = do_DELETE = do_HEAD = do_PUT = do_PATCH = do_OPTIONS = handle_request

    def log_message(self, *_):
        pass


class MockServer(ThreadingHTTPServer):
    # Join request handlers on close; each accepted socket has a bounded read timeout.
    daemon_threads = False

    def __init__(self, directory, port=0, control_token=None):
        self.catalog = Catalog(directory / "warehouse")
        self.journal = directory / "requests.jsonl"
        self.unsupported = []
        self.errors = []
        self.control_token = control_token
        self.stop_requested = threading.Event()
        super().__init__(("127.0.0.1", port), Handler)
        self.thread = threading.Thread(target=self.serve_forever, kwargs={"poll_interval": 0.05})
        self.uri = f"http://127.0.0.1:{self.server_port}"

    def get_request(self):
        request, address = super().get_request()
        request.settimeout(5)
        return request, address

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.shutdown()
        self.server_close()
        self.thread.join()
