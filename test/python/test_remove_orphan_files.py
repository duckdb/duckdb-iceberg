import json
import os
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from threading import Thread
from urllib.parse import parse_qs, quote, urlsplit
from xml.etree.ElementTree import Element, SubElement, tostring

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from scripts.mock_rest_catalog.server import MockServer


def orphan_test_config(tmp_path):
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "statically_loaded_extensions": ["core_functions", "parquet", "avro", "iceberg", "httpfs"],
                "test_env": [{"env_name": "CATALOG_TEST_CONFIG_SETUP", "env_value": "orphan_files"}],
            }
        )
    )
    return config


@pytest.mark.parametrize("delegation", ["none", "vended_credentials"])
def test_s3_deletes_old_keys_from_listing(tmp_path, unittest_binary, print_unittest_stdin, delegation):
    requests = []

    class Bucket(SimpleHTTPRequestHandler):
        def do_GET(self):
            query = parse_qs(urlsplit(self.path).query)
            if query.get("list-type") == ["2"]:
                page = 2 if query.get("continuation-token") == ["page2"] else 1
                requests.append(("LIST", page))
                self.path = f"/listing-{page}.xml"
            super().do_GET()

        def do_DELETE(self):
            self.send_response(403 if self.path.endswith("/denied") else 204)
            self.end_headers()

        def log_request(self, *args):
            if self.command == "DELETE":
                authorization = self.headers.get("Authorization", "")
                key_id = authorization.partition("Credential=")[2].partition("/")[0]
                requests.append((self.command, self.path, key_id))
            elif self.path.startswith("/warehouse/table/data/"):
                requests.append((self.command, self.path))

    # LIST can return subsecond timestamps without ETags. Repeat three keys across pages.
    pages = [
        [
            ("old", "499999"),
            ("denied", "499999"),
            ("at-cutoff", "500000"),
            # LIST names are literal. Skip ambiguous keys even when they are old enough to delete.
            ("at-cutoff?old", "499999"),
            ("at-cutoff#old", "499999"),
            ("nested/../at-cutoff", "499999"),
            ("fresh?tag", "500001"),
            ("fresh#tag", "500001"),
            ("nested/../fresh", "500001"),
            ("duplicate-old-first", "499999"),
            ("duplicate-new-first", "500000"),
            ("duplicate-no-time", None),
        ],
        [("duplicate-old-first", "500000"), ("duplicate-new-first", "499999"), ("duplicate-no-time", "499999")],
    ]
    for page, keys in enumerate(pages, 1):
        listing = Element("ListBucketResult")
        SubElement(listing, "EncodingType").text = "url"
        SubElement(listing, "IsTruncated").text = "true" if page == 1 else "false"
        objects = [(f"table/data/{key}", fraction) for key, fraction in keys]
        if page == 1:
            SubElement(listing, "NextContinuationToken").text = "page2"
            objects.append(("table/current.metadata.json", "499999"))
        for path, fraction in objects:
            entry = SubElement(listing, "Contents")
            SubElement(entry, "Key").text = quote(path)
            SubElement(entry, "Size").text = "1"
            if fraction is not None:
                SubElement(entry, "LastModified").text = f"2020-01-01T00:00:00.{fraction}Z"
        (tmp_path / f"listing-{page}.xml").write_text(tostring(listing, encoding="unicode"))
    config = orphan_test_config(tmp_path)

    with MockServer(tmp_path / "catalog") as rest, HTTPServer(
        ("127.0.0.1", 0), partial(Bucket, directory=tmp_path)
    ) as s3:
        rest.catalog.create_namespace({"namespace": ["default"]})
        schema = {"type": "struct", "fields": [{"id": 1, "name": "id", "type": "int", "required": False}]}
        response = rest.catalog.create(("default",), {"name": "t", "schema": schema, "stage-create": True})
        response["metadata"]["location"] = "s3://warehouse/table"
        response["metadata-location"] = "s3://warehouse/table/current.metadata.json"
        if delegation == "vended_credentials":
            response["storage-credentials"] = [
                {
                    "prefix": "s3://warehouse/table/",
                    "config": {
                        "s3.endpoint": f"http://127.0.0.1:{s3.server_port}",
                        "s3.path-style-access": "true",
                        "s3.region": "us-east-1",
                        "s3.access-key-id": "test-key",
                        "s3.secret-access-key": "test-secret",
                    },
                }
            ]
        rest.catalog.tables[(("default",), "t")] = response
        metadata = tmp_path / "warehouse" / "table" / "current.metadata.json"
        metadata.parent.mkdir(parents=True)
        metadata.write_text(json.dumps(response["metadata"]))

        thread = Thread(target=s3.serve_forever, daemon=True)
        thread.start()
        try:
            with DuckDBUnittestRunner(unittest_binary, test_config=config, print_stdin=print_unittest_stdin) as test:
                test.statement_ok("SET s3_allow_recursive_globbing = false")
                test.statement_ok("CREATE SECRET fallback (TYPE s3, ENDPOINT 'unused.invalid', SCOPE 's3://')")
                test.statement_ok(
                    f"CREATE SECRET store (TYPE s3, ENDPOINT '127.0.0.1:{s3.server_port}', "
                    "KEY_ID 'store-key', SECRET 'store-secret', URL_STYLE 'path', USE_SSL false, SCOPE 's3://warehouse/')"
                )
                test.statement_ok(
                    f"ATTACH '' AS lake (TYPE iceberg, URI '{rest.uri}', AUTHORIZATION_TYPE 'none', "
                    f"ACCESS_DELEGATION_MODE '{delegation}')"
                )
                test.statement_ok("BEGIN")
                # The narrower store still wins for both LIST and the files it returns.
                test.statement_ok(
                    "CREATE OR REPLACE SECRET fallback (TYPE s3, ENDPOINT 'changed.invalid', SCOPE 's3://')"
                )
                if delegation == "vended_credentials":
                    test.statement_ok(
                        "CREATE OR REPLACE SECRET store (TYPE s3, ENDPOINT 'unused.invalid', SCOPE 's3://warehouse/')"
                    )
                test.statement_ok(
                    "CREATE SECRET unrelated (TYPE s3, SCOPE ['s3://another-bucket/', 's3://warehouse/other/'])"
                )
                failed_keys = ("denied", "duplicate-new-first", "duplicate-no-time", "duplicate-old-first")
                preview_paths = ",".join(f"s3://warehouse/table/data/{key}" for key in (*failed_keys, "old"))
                test.query(
                    "TIIT",
                    """
                    SELECT error_code, scanned_files, deleted_file_count, array_to_string(orphan_file_locations, ',')
                    FROM iceberg_remove_orphan_files(
                        'lake.default.t', dry_run => true, older_than => TIMESTAMP '2020-01-01 00:00:00.5'
                    )
                    """,
                    [("SUCCESS", 16, 0, preview_paths)],
                )
                test.query("T", "SELECT name FROM which_secret('s3://warehouse/table/data/old', 's3')", [("store",)])
                test.statement_ok("ROLLBACK")
                test.query(
                    "TIITT",
                    """
                    SELECT error_code, deleted_file_count, failed_file_count, parse_filename(orphan_file_locations[1]),
                           array_to_string(list_transform(failed_file_locations, lambda p: parse_filename(p)), ',')
                    FROM iceberg_remove_orphan_files('lake.default.t', older_than => TIMESTAMP '2020-01-01 00:00:00.5')
                    """,
                    [("FILE_DELETE_FAILED", 1, 4, "old", ",".join(failed_keys))],
                )
        finally:
            s3.shutdown()
            thread.join()

    expected_key_id = "test-key" if delegation == "vended_credentials" else "store-key"
    assert requests == [
        ("LIST", 1),
        ("LIST", 2),
        ("LIST", 1),
        ("LIST", 2),
        ("DELETE", "/warehouse/table/data/denied", expected_key_id),
        ("DELETE", "/warehouse/table/data/old", expected_key_id),
    ]


def test_dotted_namespace_cleanup(tmp_path, unittest_binary, print_unittest_stdin):
    with MockServer(tmp_path / "catalog") as rest:
        # Both x.y.z leaves are directly addressable without a parent namespace.
        ambiguous = (("a", "b"), ("a.b",), ("x", "y", "z"), ("x.y", "z"))
        unique = (("a", "nested"), ("literal.only",), ("free.part", "leaf"))
        markers = {}
        schema = {"type": "struct", "fields": [{"id": 1, "name": "id", "type": "int", "required": False}]}
        for namespace in (("default",), ("a",)):
            rest.catalog.create_namespace({"namespace": list(namespace)})
        for namespace in (*ambiguous, *unique):
            rest.catalog.create_namespace({"namespace": list(namespace)})
            table = rest.catalog.create(namespace, {"name": "t", "schema": schema})
            marker = Path(table["metadata"]["location"]) / "orphan.csv"
            marker.write_text("orphan")
            os.utime(marker, (1, 1))
            markers[namespace] = marker

        with DuckDBUnittestRunner(
            unittest_binary, test_config=orphan_test_config(tmp_path), print_stdin=print_unittest_stdin
        ) as test:
            for catalog in ("cold", "warm", "listed"):
                test.statement_ok(
                    f"ATTACH '' AS {catalog} (TYPE iceberg, URI '{rest.uri}', AUTHORIZATION_TYPE 'none', "
                    "ACCESS_DELEGATION_MODE 'none')"
                )
                if catalog == "warm":
                    test.query("I", 'SELECT count(*) FROM warm."a.b".t', [(0,)])
                elif catalog == "listed":
                    test.statement_ok("SELECT * FROM duckdb_schemas() WHERE database_name = 'listed'")
                for dry_run in ("true", "false"):
                    test.statement_error(
                        f"""FROM iceberg_remove_orphan_files('{catalog}."a.b".t', dry_run => {dry_run})""",
                        "ambiguous Iceberg namespace 'a.b'",
                    )

            test.statement_error(
                """FROM iceberg_remove_orphan_files('cold."x.y.z".t')""", "ambiguous Iceberg namespace 'x.y.z'"
            )
            test.statement_error("""FROM iceberg_remove_orphan_files('cold."a.b.c.d.e.f.g.h".t')""", "more than 6 dots")
            for namespace in unique:
                name = ".".join(namespace)
                test.query(
                    "TI",
                    f"""SELECT error_code, deleted_file_count FROM iceberg_remove_orphan_files('cold."{name}".t')""",
                    [("SUCCESS", 1)],
                )

        assert all(markers[ns].exists() for ns in ambiguous)
        assert all(not markers[ns].exists() for ns in unique)
