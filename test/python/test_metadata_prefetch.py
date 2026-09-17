"""Exercise metadata prefetch against a local REST server, without a warehouse.

Uses the shell and extensions from the build selected by --unittest-binary.
"""

from collections import Counter
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time
from urllib.parse import urlsplit

import pytest


TABLES = [f"table_{i:02}" for i in range(12)]


def table_metadata(name):
    return {
        "metadata-location": f"file:///unused/{name}/metadata.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": "b459766e-0a65-4dd9-986a-1a3b4388db48",
            "location": f"file:///unused/{name}",
            "last-sequence-number": 0,
            "last-updated-ms": 1,
            "last-column-id": 2,
            "current-schema-id": 0,
            "schemas": [
                {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "required": False, "type": "long", "doc": "Identifier"},
                        {"id": 2, "name": "label", "required": False, "type": "string"},
                    ],
                }
            ],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "properties": {},
        },
    }


class Catalog(ThreadingHTTPServer):
    def __init__(self, failure=None):
        super().__init__(("127.0.0.1", 0), Handler)
        self.lock = threading.Lock()
        self.requests = Counter()
        self.active = 0
        self.max_active = 0
        self.failure = failure
        self.delay = 0.05
        self.started = threading.Event()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def respond(self, status, value):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/v1/config":
            return self.respond(200, {"defaults": {}, "overrides": {}})
        if path == "/v1/namespaces":
            return self.respond(200, {"namespaces": [["default"]]})
        if path == "/v1/namespaces/default":
            return self.respond(200, {"namespace": ["default"], "properties": {}})
        if path == "/v1/namespaces/default/tables":
            return self.respond(200, {"identifiers": [{"namespace": ["default"], "name": n} for n in TABLES]})
        prefix = "/v1/namespaces/default/tables/"
        if path.startswith(prefix) and path[len(prefix) :] in TABLES:
            name = path[len(prefix) :]
            with self.server.lock:
                self.server.requests[name] += 1
                self.server.active += 1
                self.server.max_active = max(self.server.max_active, self.server.active)
                self.server.started.set()
            try:
                # Make overlap observable without asserting wall-clock performance.
                time.sleep(self.server.delay)
                if self.server.failure == "forbidden":
                    return self.respond(
                        403, {"error": {"message": "prefetch denied", "type": "ForbiddenException", "code": 403}}
                    )
                if self.server.failure == "malformed":
                    return self.respond(200, {})
                return self.respond(200, table_metadata(name))
            finally:
                with self.server.lock:
                    self.server.active -= 1
        return self.respond(404, {"error": {"message": path, "type": "NoSuchTableException", "code": 404}})


@contextmanager
def catalog_server(failure=None):
    server = Catalog(failure)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        worker.join()


@pytest.fixture()
def metadata_shell(unittest_binary, tmp_path):
    build_dir = Path(unittest_binary).resolve().parent.parent
    binary = build_dir / ("duckdb.exe" if os.name == "nt" else "duckdb")
    assert binary.is_file(), f"Missing shell {binary}; build the shell alongside --unittest-binary"
    command = [str(binary), "-unsigned", "-init", os.devnull, "-batch", "-csv", "-noheader"]
    probe = subprocess.run(
        command + ["-c", "SELECT extension_name FROM duckdb_extensions() WHERE install_mode='STATICALLY_LINKED';"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert probe.returncode == 0, probe.stderr
    static_extensions = set(probe.stdout.splitlines())

    def quote_path(path):
        return "'" + path.as_posix().replace("'", "''") + "'"

    # Load this build's extensions, including CI artifacts built without static extensions.
    # Disable downloads and isolate any local-repository installation from the user's extensions.
    setup = ["SET autoinstall_known_extensions=false;", f"SET extension_directories=[{quote_path(tmp_path)}];"]
    for name in ("parquet", "avro", "httpfs", "iceberg"):
        if name in static_extensions:
            setup.append(f"LOAD {name};")
            continue
        candidates = [
            build_dir / "test" / "extension" / f"{name}.duckdb_extension",
            build_dir / "extension" / name / f"{name}.duckdb_extension",
        ]
        extension = next((path for path in candidates if path.is_file()), None)
        if extension is not None:
            setup.append(f"LOAD {quote_path(extension)};")
        else:
            repository = build_dir / "repository"
            assert repository.is_dir(), f"Missing {name} extension in build {build_dir}"
            setup.extend([f"INSTALL {name} FROM {quote_path(repository)};", f"LOAD {name};"])
    return command, "\n".join(setup)


def shell_command(metadata_shell, server, sql, threads=4, attach_options=""):
    command, setup = metadata_shell
    attach = f"""
        SET threads={threads};
        ATTACH '' AS prefetch (TYPE ICEBERG, AUTHORIZATION_TYPE 'none',
            URI 'http://127.0.0.1:{server.server_port}' {attach_options});
    """
    return command + ["-c", setup + attach + sql]


def run_sql(metadata_shell, server, sql, threads=4, attach_options=""):
    return subprocess.run(
        shell_command(metadata_shell, server, sql, threads, attach_options),
        capture_output=True,
        text=True,
        timeout=60,
    )


@pytest.mark.parametrize("threads", [1, 4, 16])
def test_parallel_metadata_loads_keep_columns_and_oids_stable(metadata_shell, threads):
    with catalog_server() as server:
        result = run_sql(
            metadata_shell,
            server,
            """
            BEGIN;
            SELECT count(*) FROM (SHOW ALL TABLES);
            SELECT count(*) FROM (SHOW ALL TABLES);
            SELECT count(*) FROM duckdb_columns() WHERE database_name='prefetch';
            SELECT count(*) FROM duckdb_columns() WHERE database_name='prefetch' AND comment='Identifier';
            SELECT count(*) FROM duckdb_tables() t JOIN duckdb_columns() c
              USING (database_name, schema_name, table_name)
              WHERE t.database_name='prefetch' AND t.table_oid<>c.table_oid;
            COMMIT;
        """,
            threads=threads,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.splitlines() == ["12", "12", "24", "12", "0"]
        assert server.requests == Counter({name: 1 for name in TABLES})
        assert 1 <= server.max_active <= min(threads, 8)
        if threads > 1:
            assert server.max_active > 1
        assert server.active == 0


def test_attach_does_not_load_table_metadata(metadata_shell):
    with catalog_server() as server:
        result = run_sql(metadata_shell, server, "SELECT 42;")
        assert result.returncode == 0, result.stderr
        assert not server.requests


@pytest.mark.parametrize(
    "failure,message", [("forbidden", "prefetch denied"), ("malformed", "required property 'metadata'")]
)
def test_prefetch_propagates_errors_and_drains_requests(metadata_shell, failure, message):
    with catalog_server(failure) as server:
        result = run_sql(metadata_shell, server, "SHOW ALL TABLES;")
        assert result.returncode != 0
        assert message in result.stderr
        assert 1 < server.max_active <= 4
        assert server.active == 0
        assert sum(server.requests.values()) <= 4


def test_prefetch_respects_metadata_cache(metadata_shell):
    with catalog_server() as server:
        result = run_sql(
            metadata_shell,
            server,
            """
            SELECT count(*) FROM (SHOW ALL TABLES);
            DESCRIBE prefetch.default.table_00;
            SELECT count(*) FROM (SHOW ALL TABLES);
        """,
            attach_options=", MAX_TABLE_STALENESS '1 hour'",
        )
        assert result.returncode == 0, result.stderr
        assert server.requests == Counter({name: 1 for name in TABLES})


@pytest.mark.skipif(os.name == "nt", reason="Uses SIGINT to interrupt the shell")
def test_interrupt_drains_prefetch_requests(metadata_shell):
    with catalog_server() as server:
        server.delay = 0.5
        process = subprocess.Popen(
            shell_command(metadata_shell, server, "SHOW ALL TABLES;"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            assert server.started.wait(10), "No metadata request started"
            process.send_signal(signal.SIGINT)
            stdout, stderr = process.communicate(timeout=30)
            # The shell suppresses the query error after SIGINT. A normal error exit (rather
            # than termination by signal) and no active requests establish that it drained.
            assert process.returncode == 1, (stdout, stderr)
            assert server.active == 0
            assert sum(server.requests.values()) <= 4
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()
