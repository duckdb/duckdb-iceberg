"""mitmproxy addon for Iceberg server-side scan planning error tests.

Run with:

    mitmdump --mode regular@19135 -s scripts/scan_planning_error_proxy.py

The addon adds the scan planning endpoint to the fixture catalog's /v1/config
response and answers scan planning requests itself, based on the table name:

- plan_forbidden: HTTP 403, as for a caller that may not plan scans of the table
- plan_failed: a planning result with status "failed"
- any other table: HTTP 406, as for a catalog that does not plan the request

All other requests reach the fixture catalog and the object store unchanged.
"""

import json
import re
import urllib.parse

from mitmproxy import http

CATALOG_HOST = "127.0.0.1"
CATALOG_PORT = 8181

PLAN_ENDPOINT = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"

FORBIDDEN_TABLE = "plan_forbidden"
FAILED_TABLE = "plan_failed"


class ScanPlanningErrorAddon:
    def request(self, flow: http.HTTPFlow):
        if not self._is_catalog_request(flow) or flow.request.method != "POST":
            return
        path = urllib.parse.urlparse(flow.request.path).path
        plan_match = re.fullmatch(r"/v1/namespaces/[^/]+/tables/([^/]+)/plan", path)
        if not plan_match:
            return

        table = plan_match.group(1)
        if table == FORBIDDEN_TABLE:
            flow.response = self._json_response(
                403, self._error("Not allowed to plan scans of this table", "ForbiddenException", 403)
            )
        elif table == FAILED_TABLE:
            body = self._error("Scan planning failed on the server", "RuntimeException", 500)
            body["status"] = "failed"
            flow.response = self._json_response(200, body)
        else:
            flow.response = self._json_response(
                406, self._error("Scan planning is not supported for this table", "UnsupportedOperationException", 406)
            )

    def response(self, flow: http.HTTPFlow):
        if not self._is_catalog_request(flow) or flow.request.method != "GET":
            return
        if urllib.parse.urlparse(flow.request.path).path != "/v1/config":
            return
        if not flow.response or flow.response.status_code != 200:
            return

        config = json.loads(flow.response.content.decode())
        endpoints = config.get("endpoints")
        if endpoints is not None and PLAN_ENDPOINT not in endpoints:
            endpoints.append(PLAN_ENDPOINT)
            flow.response.text = json.dumps(config)

    @staticmethod
    def _is_catalog_request(flow: http.HTTPFlow):
        return flow.request.host == CATALOG_HOST and flow.request.port == CATALOG_PORT

    @staticmethod
    def _error(message, error_type, code):
        return {"error": {"message": message, "type": error_type, "code": code}}

    @staticmethod
    def _json_response(status, body):
        data = json.dumps(body).encode()
        return http.Response.make(
            status,
            data,
            {"Content-Type": "application/json", "Content-Length": str(len(data))},
        )


addons = [ScanPlanningErrorAddon()]
