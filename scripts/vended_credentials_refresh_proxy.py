"""mitmproxy addon for deterministic Iceberg vended credential refresh tests.

Run with:

    mitmdump --mode regular@19133 -s scripts/vended_credentials_refresh_proxy.py

The addon rewrites selected real fixture catalog table responses to vend
controlled S3 credentials, then intercepts MinIO requests to reject stale
credentials and serve deterministic local Parquet objects.

For refresh tests, the proxy returns initial credentials on the first table load,
rejects an S3 request signed with those credentials, and then returns refreshed
credentials on the refresh-triggered table load. This models the production
contract that fresh vended credentials come from reloading the table through the
Iceberg REST catalog, without waiting for a real STS expiry in CI.
"""

import datetime
import hashlib
import hmac
import json
import re
import urllib.parse
from pathlib import Path

from mitmproxy import http


CATALOG_HOST = "127.0.0.1"
CATALOG_PORT = 8181
S3_HOST = "127.0.0.1"
S3_PORT = 9000

SCAN_TABLE = "empty_table"
TABLE_SCAN_TABLE = "vended_refresh_table"
INIT_TABLE = "vended_init_refresh"
RANGE_TABLE = "vended_range_refresh"
HEAD_TABLE = "vended_head_refresh"
GET_TABLE = "vended_get_refresh"
PUT_TABLE = "vended_put_refresh"
POST_TABLE = "vended_post_refresh"
LIST_TABLE = "vended_list_refresh"
DELETE_TABLE = "vended_delete_refresh"
DIRECT_DELETE_TABLE = "vended_direct_delete_refresh"
MIXED_REFRESH_A_TABLE = "vended_mixed_delete_refresh_a"
MIXED_REFRESH_B_TABLE = "vended_mixed_delete_refresh_b"
SAME_BAD_TABLE = "vended_same_bad_refresh"
RANGE_FAIL_TABLE = "vended_range_fail_refresh"

OLD_SCAN_KEY = "OLD_SCAN_KEY"
NEW_SCAN_KEY = "NEW_SCAN_KEY"
INITIAL_TABLE_SCAN_KEY = "INITIAL_TABLE_SCAN_KEY"
REFRESHED_TABLE_SCAN_KEY = "REFRESHED_TABLE_SCAN_KEY"
OLD_INIT_KEY = "OLD_INIT_KEY"
NEW_INIT_KEY = "NEW_INIT_KEY"
OLD_RANGE_KEY = "OLD_RANGE_KEY"
NEW_RANGE_KEY = "NEW_RANGE_KEY"
OLD_HEAD_KEY = "OLD_HEAD_KEY"
NEW_HEAD_KEY = "NEW_HEAD_KEY"
OLD_GET_KEY = "OLD_GET_KEY"
NEW_GET_KEY = "NEW_GET_KEY"
OLD_PUT_KEY = "OLD_PUT_KEY"
NEW_PUT_KEY = "NEW_PUT_KEY"
OLD_POST_KEY = "OLD_POST_KEY"
NEW_POST_KEY = "NEW_POST_KEY"
OLD_LIST_KEY = "OLD_LIST_KEY"
NEW_LIST_KEY = "NEW_LIST_KEY"
OLD_DELETE_KEY = "OLD_DELETE_KEY"
NEW_DELETE_KEY = "NEW_DELETE_KEY"
OLD_DIRECT_DELETE_KEY = "OLD_DIRECT_DELETE_KEY"
NEW_DIRECT_DELETE_KEY = "NEW_DIRECT_DELETE_KEY"
MIXED_DELETE_ROOT_KEY = "MIXED_DELETE_ROOT_KEY"
MIXED_DELETE_A_KEY = "MIXED_DELETE_A_KEY"
MIXED_DELETE_B_KEY = "MIXED_DELETE_B_KEY"
OLD_MIXED_REFRESH_KEY = "OLD_MIXED_REFRESH_KEY"
NEW_MIXED_REFRESH_A_KEY = "NEW_MIXED_REFRESH_A_KEY"
NEW_MIXED_REFRESH_B_KEY = "NEW_MIXED_REFRESH_B_KEY"
OLD_SAME_BAD_KEY = "OLD_SAME_BAD_KEY"
OLD_RANGE_FAIL_KEY = "OLD_RANGE_FAIL_KEY"
NEW_RANGE_FAIL_KEY = "NEW_RANGE_FAIL_KEY"

UPSTREAM_S3_KEY = "admin"
UPSTREAM_S3_SECRET = "password"
UPSTREAM_S3_REGION = "us-east-1"

SCRIPT_DIR = Path(__file__).resolve().parent
LOCAL_TINY_PARQUET_SOURCE = (
    SCRIPT_DIR.parent
    / "data/persistent/expression_filter/data/00000-0-1406cdaa-c3e4-4e6d-a22b-d85e4a813169-00001.parquet"
)
LOCAL_RANGE_PARQUET_SOURCE = (
    SCRIPT_DIR.parent
    / "data/persistent/iceberg/lineitem_iceberg_gz/data/00000-2-371a340c-ded5-4e85-aa49-9c788d6f21cd-00001.parquet"
)
LOCAL_PARQUET_OBJECTS = {
    "/warehouse/vended_credentials_refresh/init/data.parquet": LOCAL_TINY_PARQUET_SOURCE,
    "/warehouse/vended_credentials_refresh/range/data.parquet": LOCAL_RANGE_PARQUET_SOURCE,
    "/warehouse/vended_credentials_refresh/head/data.parquet": LOCAL_TINY_PARQUET_SOURCE,
    "/warehouse/vended_credentials_refresh/get/data.parquet": LOCAL_TINY_PARQUET_SOURCE,
    "/warehouse/vended_credentials_refresh/same_bad/data.parquet": LOCAL_TINY_PARQUET_SOURCE,
    "/warehouse/vended_credentials_refresh/range_fail/data.parquet": LOCAL_RANGE_PARQUET_SOURCE,
}


class VendedCredentialRefreshAddon:
    def __init__(self):
        self.refresh_unlocked = {
            SCAN_TABLE: False,
            TABLE_SCAN_TABLE: False,
            INIT_TABLE: False,
            RANGE_TABLE: False,
            HEAD_TABLE: False,
            GET_TABLE: False,
            PUT_TABLE: False,
            POST_TABLE: False,
            LIST_TABLE: False,
            DELETE_TABLE: False,
            DIRECT_DELETE_TABLE: False,
            MIXED_REFRESH_A_TABLE: False,
            MIXED_REFRESH_B_TABLE: False,
            SAME_BAD_TABLE: False,
            RANGE_FAIL_TABLE: False,
        }
        self.table_scan_refresh_path = None
        # Force a rollback if this branch reaches the catalog commit after writing the
        # direct-delete data file. Some fixture versions fail earlier while building metadata.
        self.direct_delete_data_written = False

    def request(self, flow: http.HTTPFlow):
        if self._is_catalog_request(flow):
            self._handle_catalog_request(flow)
            return
        if self._is_s3_request(flow):
            self._handle_s3_request(flow)

    def response(self, flow: http.HTTPFlow):
        table = flow.metadata.get("vended_table")
        if not table or not flow.response or flow.response.status_code >= 300:
            return

        response = json.loads(flow.response.content.decode())
        response["storage-credentials"] = [self._credentials_for_table(table)]
        flow.response.text = json.dumps(response)

    @staticmethod
    def _is_catalog_request(flow: http.HTTPFlow):
        return flow.request.host == CATALOG_HOST and flow.request.port == CATALOG_PORT

    @staticmethod
    def _is_s3_request(flow: http.HTTPFlow):
        return flow.request.host == S3_HOST and flow.request.port == S3_PORT

    def _handle_catalog_request(self, flow: http.HTTPFlow):
        parsed_path = urllib.parse.urlparse(flow.request.path)
        path = parsed_path.path

        table_match = re.fullmatch(r"/v1/namespaces/default/tables/([^/]+)", path)
        credentials_match = re.fullmatch(r"/v1/namespaces/default/tables/([^/]+)/credentials", path)

        if self._is_direct_delete_commit_request(flow, path):
            self._forced_catalog_commit_failure(flow)
            return

        if flow.request.method == "GET" and table_match and table_match.group(1) in self.refresh_unlocked:
            table = table_match.group(1)
            flow.metadata["vended_table"] = table
            return

        if credentials_match and credentials_match.group(1) in self.refresh_unlocked:
            table = credentials_match.group(1)
            body = json.dumps({"storage-credentials": [self._credentials_for_table(table)]}).encode()
            flow.response = http.Response.make(200, body, {"Content-Type": "application/json"})

    def _is_direct_delete_commit_request(self, flow: http.HTTPFlow, path):
        if flow.request.method != "POST":
            return False
        if not self.direct_delete_data_written:
            return False
        if path != f"/v1/namespaces/default/tables/{DIRECT_DELETE_TABLE}" and path != "/v1/transactions/commit":
            return False
        if path == f"/v1/namespaces/default/tables/{DIRECT_DELETE_TABLE}":
            return True
        return DIRECT_DELETE_TABLE in flow.request.text

    @staticmethod
    def _forced_catalog_commit_failure(flow: http.HTTPFlow):
        body = json.dumps(
            {
                "error": {
                    "message": "forced direct delete refresh rollback",
                    "type": "TestFailure",
                    "code": 500,
                    "stack": [],
                }
            }
        ).encode()
        flow.response = http.Response.make(500, body, {"Content-Type": "application/json"})

    def _handle_s3_request(self, flow: http.HTTPFlow):
        if self._is_direct_delete_data_write(flow):
            self.direct_delete_data_written = True

        key_id = self._extract_access_key(flow.request.headers)
        has_range = flow.request.headers.get("Range") is not None
        is_full_get = flow.request.method == "GET" and not has_range and not self._is_list_request(flow)
        is_put = flow.request.method == "PUT"
        is_multipart_post = flow.request.method == "POST" and self._has_query_key(flow, "uploads")
        is_delete_post = flow.request.method == "POST" and self._has_query_key(flow, "delete")

        if key_id == OLD_SCAN_KEY:
            self.refresh_unlocked[SCAN_TABLE] = True
            self._forbidden(flow, "stale scan credentials")
            return
        if key_id == INITIAL_TABLE_SCAN_KEY and self._is_data_file_request(flow):
            data_path = urllib.parse.urlparse(flow.request.path).path
            if self.table_scan_refresh_path is None:
                self.table_scan_refresh_path = data_path
                self.refresh_unlocked[TABLE_SCAN_TABLE] = True
                self._forbidden(flow, "stale table scan credentials")
                return
            if data_path == self.table_scan_refresh_path:
                self._forbidden(flow, "stale table scan credentials")
                return
        if key_id == OLD_INIT_KEY:
            self.refresh_unlocked[INIT_TABLE] = True
            self._forbidden(flow, "stale init credentials")
            return
        if key_id == OLD_RANGE_KEY and flow.request.method == "GET" and has_range:
            self.refresh_unlocked[RANGE_TABLE] = True
            self._forbidden(flow, "stale range credentials")
            return
        if key_id == OLD_HEAD_KEY and flow.request.method == "HEAD":
            self.refresh_unlocked[HEAD_TABLE] = True
            self._forbidden(flow, "stale head credentials")
            return
        if key_id == OLD_GET_KEY and is_full_get:
            self.refresh_unlocked[GET_TABLE] = True
            self._forbidden(flow, "stale get credentials")
            return
        if key_id == OLD_PUT_KEY and is_put:
            self.refresh_unlocked[PUT_TABLE] = True
            self._forbidden(flow, "stale put credentials")
            return
        if key_id == OLD_POST_KEY and is_multipart_post:
            self.refresh_unlocked[POST_TABLE] = True
            self._forbidden(flow, "stale multipart post credentials")
            return
        if key_id == OLD_LIST_KEY and self._is_list_request(flow):
            self.refresh_unlocked[LIST_TABLE] = True
            self._forbidden(flow, "stale list credentials")
            return
        if key_id == OLD_DELETE_KEY and is_delete_post:
            self.refresh_unlocked[DELETE_TABLE] = True
            self._forbidden(flow, "stale delete credentials")
            return
        if key_id == OLD_DIRECT_DELETE_KEY and flow.request.method == "DELETE":
            self.refresh_unlocked[DIRECT_DELETE_TABLE] = True
            self._forbidden(flow, "stale direct delete credentials")
            return
        if key_id == OLD_MIXED_REFRESH_KEY and is_delete_post and self._is_mixed_refresh_delete_request(flow):
            has_a, has_b = self._mixed_refresh_delete_parts(flow)
            if has_a and not has_b:
                self.refresh_unlocked[MIXED_REFRESH_A_TABLE] = True
                self._forbidden(flow, "stale mixed refresh partition a credentials")
                return
            if has_b and not has_a:
                self.refresh_unlocked[MIXED_REFRESH_B_TABLE] = True
                self._forbidden(flow, "stale mixed refresh partition b credentials")
                return
            self._forbidden(flow, "mixed refresh credentials batched together")
            return
        if key_id == OLD_SAME_BAD_KEY:
            self.refresh_unlocked[SAME_BAD_TABLE] = True
            self._forbidden(flow, "same bad credentials")
            return
        if key_id == OLD_RANGE_FAIL_KEY and flow.request.method == "GET" and has_range:
            self.refresh_unlocked[RANGE_FAIL_TABLE] = True
            self._forbidden(flow, "stale range-fail credentials")
            return
        if key_id == NEW_RANGE_FAIL_KEY:
            self._forbidden(flow, "refreshed range-fail credentials")
            return
        if is_delete_post and self._is_mixed_delete_request(flow):
            if not self._validate_mixed_delete_request(flow, key_id):
                return
        if is_delete_post and self._is_mixed_refresh_delete_request(flow):
            if not self._validate_mixed_refresh_delete_request(flow, key_id):
                return
        if key_id not in {
            NEW_SCAN_KEY,
            INITIAL_TABLE_SCAN_KEY,
            REFRESHED_TABLE_SCAN_KEY,
            NEW_INIT_KEY,
            OLD_RANGE_KEY,
            NEW_RANGE_KEY,
            OLD_HEAD_KEY,
            NEW_HEAD_KEY,
            OLD_GET_KEY,
            NEW_GET_KEY,
            OLD_PUT_KEY,
            NEW_PUT_KEY,
            OLD_POST_KEY,
            NEW_POST_KEY,
            OLD_LIST_KEY,
            NEW_LIST_KEY,
            OLD_DELETE_KEY,
            NEW_DELETE_KEY,
            OLD_DIRECT_DELETE_KEY,
            NEW_DIRECT_DELETE_KEY,
            MIXED_DELETE_ROOT_KEY,
            MIXED_DELETE_A_KEY,
            MIXED_DELETE_B_KEY,
            OLD_MIXED_REFRESH_KEY,
            NEW_MIXED_REFRESH_A_KEY,
            NEW_MIXED_REFRESH_B_KEY,
            OLD_RANGE_FAIL_KEY,
        }:
            self._forbidden(flow, "unknown test credentials")
            return

        local_object = LOCAL_PARQUET_OBJECTS.get(urllib.parse.urlparse(flow.request.path).path)
        if local_object:
            self._serve_local_object(flow, local_object)
            return

        self._resign_s3_request(flow)

    @staticmethod
    def _is_direct_delete_data_write(flow: http.HTTPFlow):
        return flow.request.method == "PUT" and urllib.parse.urlparse(flow.request.path).path.startswith(
            "/warehouse/default/vended_direct_delete_refresh/data/"
        )

    @staticmethod
    def _query_keys(flow: http.HTTPFlow):
        parsed = urllib.parse.urlparse(flow.request.path)
        return {key for key, _ in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)}

    @staticmethod
    def _has_query_key(flow: http.HTTPFlow, key):
        return key in VendedCredentialRefreshAddon._query_keys(flow)

    @staticmethod
    def _is_list_request(flow: http.HTTPFlow):
        return flow.request.method == "GET" and VendedCredentialRefreshAddon._has_query_key(flow, "list-type")

    @staticmethod
    def _is_data_file_request(flow: http.HTTPFlow):
        return "/data/" in urllib.parse.urlparse(flow.request.path).path

    @staticmethod
    def _is_mixed_delete_request(flow: http.HTTPFlow):
        return (
            flow.request.method == "POST"
            and VendedCredentialRefreshAddon._has_query_key(flow, "delete")
            and b"vended_credentials_refresh/mixed_delete/" in flow.request.content
        )

    @staticmethod
    def _is_mixed_refresh_delete_request(flow: http.HTTPFlow):
        return (
            flow.request.method == "POST"
            and VendedCredentialRefreshAddon._has_query_key(flow, "delete")
            and b"vended_credentials_refresh/mixed_delete_refresh/" in flow.request.content
        )

    @staticmethod
    def _mixed_refresh_delete_parts(flow: http.HTTPFlow):
        body = flow.request.content
        has_a = b"vended_credentials_refresh/mixed_delete_refresh/part_col=a/" in body
        has_b = b"vended_credentials_refresh/mixed_delete_refresh/part_col=b/" in body
        return has_a, has_b

    def _validate_mixed_delete_request(self, flow: http.HTTPFlow, key_id):
        body = flow.request.content
        has_a = b"vended_credentials_refresh/mixed_delete/part_col=a/" in body
        has_b = b"vended_credentials_refresh/mixed_delete/part_col=b/" in body

        if has_a and has_b:
            self._forbidden(flow, "mixed delete credentials batched together")
            return False
        if has_a and key_id != MIXED_DELETE_A_KEY:
            self._forbidden(flow, "mixed delete partition a used wrong credentials")
            return False
        if has_b and key_id != MIXED_DELETE_B_KEY:
            self._forbidden(flow, "mixed delete partition b used wrong credentials")
            return False
        return True

    def _validate_mixed_refresh_delete_request(self, flow: http.HTTPFlow, key_id):
        has_a, has_b = self._mixed_refresh_delete_parts(flow)

        if has_a and has_b:
            self._forbidden(flow, "mixed refresh delete credentials batched together")
            return False
        if has_a and key_id != NEW_MIXED_REFRESH_A_KEY:
            self._forbidden(flow, "mixed refresh partition a used wrong credentials")
            return False
        if has_b and key_id != NEW_MIXED_REFRESH_B_KEY:
            self._forbidden(flow, "mixed refresh partition b used wrong credentials")
            return False
        return True

    def _credentials_for_table(self, table):
        key_id = self._key_for_table(table)
        return {
            "prefix": self._credentials_prefix(table),
            "config": {
                "s3.access-key-id": key_id,
                "s3.secret-access-key": f"{key_id}_SECRET",
                "s3.region": "us-east-1",
                "s3.endpoint": f"http://{S3_HOST}:{S3_PORT}",
                "s3.path-style-access": "true",
            },
        }

    def _key_for_table(self, table):
        if table == SCAN_TABLE:
            return NEW_SCAN_KEY if self.refresh_unlocked[table] else OLD_SCAN_KEY
        if table == TABLE_SCAN_TABLE:
            return REFRESHED_TABLE_SCAN_KEY if self.refresh_unlocked[table] else INITIAL_TABLE_SCAN_KEY
        if table == INIT_TABLE:
            return NEW_INIT_KEY if self.refresh_unlocked[table] else OLD_INIT_KEY
        if table == RANGE_TABLE:
            return NEW_RANGE_KEY if self.refresh_unlocked[table] else OLD_RANGE_KEY
        if table == HEAD_TABLE:
            return NEW_HEAD_KEY if self.refresh_unlocked[table] else OLD_HEAD_KEY
        if table == GET_TABLE:
            return NEW_GET_KEY if self.refresh_unlocked[table] else OLD_GET_KEY
        if table == PUT_TABLE:
            return NEW_PUT_KEY if self.refresh_unlocked[table] else OLD_PUT_KEY
        if table == POST_TABLE:
            return NEW_POST_KEY if self.refresh_unlocked[table] else OLD_POST_KEY
        if table == LIST_TABLE:
            return NEW_LIST_KEY if self.refresh_unlocked[table] else OLD_LIST_KEY
        if table == DELETE_TABLE:
            return NEW_DELETE_KEY if self.refresh_unlocked[table] else OLD_DELETE_KEY
        if table == DIRECT_DELETE_TABLE:
            return NEW_DIRECT_DELETE_KEY if self.refresh_unlocked[table] else OLD_DIRECT_DELETE_KEY
        if table == MIXED_REFRESH_A_TABLE:
            return NEW_MIXED_REFRESH_A_KEY if self.refresh_unlocked[table] else OLD_MIXED_REFRESH_KEY
        if table == MIXED_REFRESH_B_TABLE:
            return NEW_MIXED_REFRESH_B_KEY if self.refresh_unlocked[table] else OLD_MIXED_REFRESH_KEY
        if table == SAME_BAD_TABLE:
            return OLD_SAME_BAD_KEY
        if table == RANGE_FAIL_TABLE:
            return NEW_RANGE_FAIL_KEY if self.refresh_unlocked[table] else OLD_RANGE_FAIL_KEY
        raise ValueError(f"unexpected table: {table}")

    @staticmethod
    def _credentials_prefix(table):
        if table == SCAN_TABLE:
            return "s3://warehouse/"
        if table == TABLE_SCAN_TABLE:
            return "s3://warehouse/"
        if table == INIT_TABLE:
            return "s3://warehouse/vended_credentials_refresh/init"
        if table == RANGE_TABLE:
            return "s3://warehouse/vended_credentials_refresh/range"
        if table == HEAD_TABLE:
            return "s3://warehouse/vended_credentials_refresh/head"
        if table == GET_TABLE:
            return "s3://warehouse/vended_credentials_refresh/get"
        if table == PUT_TABLE:
            return "s3://warehouse/vended_credentials_refresh/put"
        if table == POST_TABLE:
            return "s3://warehouse/vended_credentials_refresh/post"
        if table == LIST_TABLE:
            return "s3://warehouse/vended_credentials_refresh/list"
        if table == DELETE_TABLE:
            return "s3://warehouse/vended_credentials_refresh/delete"
        if table == DIRECT_DELETE_TABLE:
            return "s3://warehouse/"
        if table == MIXED_REFRESH_A_TABLE:
            return "s3://warehouse/vended_credentials_refresh/mixed_delete_refresh/part_col=a"
        if table == MIXED_REFRESH_B_TABLE:
            return "s3://warehouse/vended_credentials_refresh/mixed_delete_refresh/part_col=b"
        if table == SAME_BAD_TABLE:
            return "s3://warehouse/vended_credentials_refresh/same_bad"
        if table == RANGE_FAIL_TABLE:
            return "s3://warehouse/vended_credentials_refresh/range_fail"
        raise ValueError(f"unexpected table: {table}")

    @staticmethod
    def _extract_access_key(headers):
        auth = headers.get("Authorization", "")
        match = re.search(r"Credential=([^/]+)/", auth)
        return match.group(1) if match else ""

    @staticmethod
    def _forbidden(flow: http.HTTPFlow, message):
        body = (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<Error><Code>AccessDenied</Code>"
            f"<Message>{message}</Message></Error>"
        ).encode()
        flow.response = http.Response.make(
            403,
            body,
            {"Content-Type": "application/xml", "Content-Length": str(len(body))},
        )

    def _serve_local_object(self, flow: http.HTTPFlow, file_path: Path):
        if not file_path.exists():
            body = f"Missing local fixture object: {file_path}".encode()
            flow.response = http.Response.make(
                404,
                body,
                {"Content-Type": "text/plain", "Content-Length": str(len(body))},
            )
            return

        file_size = file_path.stat().st_size
        try:
            byte_range = self._parse_range_header(flow.request.headers.get("Range"), file_size)
        except ValueError:
            flow.response = http.Response.make(
                416,
                b"",
                {"Content-Range": f"bytes */{file_size}", "Content-Length": "0"},
            )
            return

        headers = {"Accept-Ranges": "bytes", "Content-Type": "application/octet-stream"}
        if flow.request.method == "HEAD":
            flow.response = http.Response.make(200, b"", headers)
            flow.response.headers["Content-Length"] = str(file_size)
            return

        if byte_range:
            start, end = byte_range
            content_length = end - start + 1
            headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
            headers["Content-Length"] = str(content_length)
            with file_path.open("rb") as f:
                f.seek(start)
                body = f.read(content_length)
            flow.response = http.Response.make(206, body, headers)
            return

        headers["Content-Length"] = str(file_size)
        body = file_path.read_bytes()
        flow.response = http.Response.make(200, body, headers)

    @staticmethod
    def _parse_range_header(range_header, file_size):
        if not range_header:
            return None
        if not range_header.startswith("bytes="):
            raise ValueError("unsupported range unit")
        range_spec = range_header[len("bytes=") :]
        if "," in range_spec:
            raise ValueError("multiple ranges are not supported")
        start_text, separator, end_text = range_spec.partition("-")
        if separator != "-":
            raise ValueError("invalid range")
        if not start_text:
            suffix_size = int(end_text)
            if suffix_size <= 0:
                raise ValueError("invalid suffix range")
            start = max(file_size - suffix_size, 0)
            end = file_size - 1
        else:
            start = int(start_text)
            end = int(end_text) if end_text else file_size - 1
        if start < 0 or end < start or start >= file_size:
            raise ValueError("range outside file")
        return start, min(end, file_size - 1)

    def _resign_s3_request(self, flow: http.HTTPFlow):
        preserved_headers = {}
        for header in ["Range", "Content-Length", "Content-Type", "Content-MD5"]:
            value = flow.request.headers.get(header)
            if value is not None:
                preserved_headers[header] = value
        flow.request.headers.clear()
        flow.request.headers.update(self._signed_s3_headers(flow.request.method, flow.request.path))
        flow.request.headers.update(preserved_headers)
        flow.request.headers["Host"] = f"{S3_HOST}:{S3_PORT}"

    @staticmethod
    def _signed_s3_headers(method, path):
        now = datetime.datetime.utcnow()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        payload_hash = "UNSIGNED-PAYLOAD"
        host_header = f"{S3_HOST}:{S3_PORT}"
        parsed_path = urllib.parse.urlparse(path)
        canonical_uri = urllib.parse.quote(parsed_path.path or "/", safe="/-_.~%")
        canonical_query = VendedCredentialRefreshAddon._canonical_query_string(parsed_path.query)
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical_headers = (
            "\n".join(
                [
                    f"host:{host_header}",
                    f"x-amz-content-sha256:{payload_hash}",
                    f"x-amz-date:{amz_date}",
                ]
            )
            + "\n"
        )
        canonical_request = "\n".join(
            [
                method,
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )
        credential_scope = f"{date_stamp}/{UPSTREAM_S3_REGION}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode()).hexdigest(),
            ]
        )
        signature = hmac.new(
            VendedCredentialRefreshAddon._signing_key(UPSTREAM_S3_SECRET, date_stamp, UPSTREAM_S3_REGION),
            string_to_sign.encode(),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "Authorization": (
                "AWS4-HMAC-SHA256 "
                f"Credential={UPSTREAM_S3_KEY}/{credential_scope}, "
                f"SignedHeaders={signed_headers}, "
                f"Signature={signature}"
            ),
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        return headers

    @staticmethod
    def _canonical_query_string(query):
        values = urllib.parse.parse_qsl(query, keep_blank_values=True)
        encoded = [
            (
                urllib.parse.quote(key, safe="-_.~"),
                urllib.parse.quote(value, safe="-_.~"),
            )
            for key, value in values
        ]
        return "&".join(f"{key}={value}" for key, value in sorted(encoded))

    @staticmethod
    def _hmac_sha256(key, message):
        return hmac.new(key, message.encode(), hashlib.sha256).digest()

    @staticmethod
    def _signing_key(secret, date_stamp, region):
        date_key = VendedCredentialRefreshAddon._hmac_sha256(("AWS4" + secret).encode(), date_stamp)
        region_key = VendedCredentialRefreshAddon._hmac_sha256(date_key, region)
        service_key = VendedCredentialRefreshAddon._hmac_sha256(region_key, "s3")
        return VendedCredentialRefreshAddon._hmac_sha256(service_key, "aws4_request")


addons = [VendedCredentialRefreshAddon()]
