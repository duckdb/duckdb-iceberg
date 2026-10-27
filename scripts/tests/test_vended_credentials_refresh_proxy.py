import json
import unittest
from types import SimpleNamespace

from mitmproxy import http

from scripts.vended_credentials_refresh_proxy import (
    CREDENTIALS_ENDPOINT,
    INIT_TABLE,
    NEW_INIT_KEY,
    OLD_INIT_KEY,
    TABLE_SCAN_TABLE,
    VendedCredentialRefreshAddon,
)


def catalog_flow(path, mode=None, copies=1):
    headers = http.Headers([] if mode is None else [(b"x-credential-endpoint", mode.encode())] * copies)
    return SimpleNamespace(
        request=http.Request.make("GET", "http://127.0.0.1:8181" + path, headers=headers),
        response=None,
        metadata={},
    )


class TestCredentialEndpointModes(unittest.TestCase):
    def test_config_modes_with_duplicate_headers(self):
        for copies in (1, 2):
            for mode in ("supported", "absent", "omitted", "error"):
                with self.subTest(copies=copies, mode=mode):
                    addon = VendedCredentialRefreshAddon()
                    flow = catalog_flow("/v1/config", mode, copies)
                    addon.request(flow)
                    flow.response = http.Response.make(
                        200,
                        json.dumps({"endpoints": ["GET /v1/{prefix}/namespaces", CREDENTIALS_ENDPOINT]}).encode(),
                    )
                    addon.response(flow)
                    config = json.loads(flow.response.text)
                    if mode == "omitted":
                        self.assertNotIn("endpoints", config)
                    else:
                        self.assertIn("GET /v1/{prefix}/namespaces", config["endpoints"])
                        self.assertEqual(
                            config["endpoints"].count(CREDENTIALS_ENDPOINT), int(mode in ("supported", "error"))
                        )

    def test_error_mode_with_duplicate_headers(self):
        for copies in (1, 2):
            with self.subTest(copies=copies):
                addon = VendedCredentialRefreshAddon()
                flow = catalog_flow("/v1/namespaces/default/tables/vended_init_refresh/credentials", "error", copies)
                addon.request(flow)
                self.assertEqual(flow.response.status_code, 403)
                self.assertEqual(json.loads(flow.response.text)["error"]["message"], "credential endpoint denied")

    def test_default_mode_supports_credentials(self):
        addon = VendedCredentialRefreshAddon()
        flow = catalog_flow("/v1/config")
        addon.request(flow)
        flow.response = http.Response.make(200, b"{}")
        addon.response(flow)
        self.assertIn(CREDENTIALS_ENDPOINT, json.loads(flow.response.text)["endpoints"])
        flow = catalog_flow("/v1/namespaces/default/tables/vended_init_refresh/credentials")
        addon.request(flow)
        self.assertEqual(flow.response.status_code, 200)

    def test_unadvertised_credentials_endpoint_is_unavailable(self):
        for mode in ("absent", "omitted"):
            with self.subTest(mode=mode):
                addon = VendedCredentialRefreshAddon()
                flow = catalog_flow(f"/v1/namespaces/default/tables/{INIT_TABLE}/credentials", mode)
                addon.request(flow)
                self.assertEqual(flow.response.status_code, 404)

    def test_config_resets_expiration_for_each_scenario(self):
        addon = VendedCredentialRefreshAddon()
        addon.refresh_unlocked[INIT_TABLE] = True
        addon.refresh_unlocked[TABLE_SCAN_TABLE] = True
        addon.table_scan_refresh_path = "/warehouse/previous/data/test.parquet"
        flow = catalog_flow("/v1/config?warehouse=test", "absent")
        addon.request(flow)
        self.assertFalse(addon.refresh_unlocked[INIT_TABLE])
        self.assertFalse(addon.refresh_unlocked[TABLE_SCAN_TABLE])
        self.assertIsNone(addon.table_scan_refresh_path)

    def test_load_table_credentials_change_only_after_s3_rejection(self):
        addon = VendedCredentialRefreshAddon()
        for expected_key in (OLD_INIT_KEY, NEW_INIT_KEY):
            flow = catalog_flow(f"/v1/namespaces/default/tables/{INIT_TABLE}", "absent")
            addon.request(flow)
            flow.response = http.Response.make(200, b'{"metadata": {"location": "s3://warehouse/default/table"}}')
            addon.response(flow)
            response = json.loads(flow.response.text)
            self.assertEqual(response["storage-credentials"][0]["config"]["s3.access-key-id"], expected_key)
            self.assertEqual(response["metadata"]["location"], response["storage-credentials"][0]["prefix"])
            stale_request = SimpleNamespace(
                request=http.Request.make(
                    "GET",
                    "http://127.0.0.1:9000/warehouse/vended_credentials_refresh/init/data.parquet",
                    headers={"Authorization": f"AWS4-HMAC-SHA256 Credential={OLD_INIT_KEY}/test"},
                ),
                response=None,
                metadata={},
            )
            addon.request(stale_request)
            self.assertEqual(stale_request.response.status_code, 403)

    def test_refresh_errors_do_not_affect_initial_load_or_get_rewritten(self):
        for endpoint in ("supported", "absent"):
            for status in (403, 404):
                with self.subTest(endpoint=endpoint, status=status):
                    addon = VendedCredentialRefreshAddon()
                    suffix = "/credentials" if endpoint == "supported" else ""
                    flow = catalog_flow(f"/v1/namespaces/default/tables/{INIT_TABLE}{suffix}", endpoint)
                    flow.request.headers.add("x-refresh-response", str(status))
                    flow.request.headers.add("x-refresh-response", str(status))
                    addon.request(flow)
                    self.assertTrue(flow.response is None or flow.response.status_code == 200)
                    addon.refresh_unlocked[INIT_TABLE] = True
                    addon.request(flow)
                    addon.response(flow)
                    self.assertEqual(flow.response.status_code, status)
                    self.assertEqual(json.loads(flow.response.text)["error"]["message"], f"refresh denied ({status})")

    def test_load_table_response_shapes(self):
        for mode in ("single", "multiple", "empty", "omitted", "invalid", "config", "merged", "removed_config"):
            with self.subTest(mode=mode):
                addon = VendedCredentialRefreshAddon()
                addon.refresh_unlocked[INIT_TABLE] = True
                flow = catalog_flow(f"/v1/namespaces/default/tables/{INIT_TABLE}", "absent")
                flow.request.headers.add("x-refresh-response", mode)
                flow.request.headers.add("x-refresh-response", mode)
                addon.request(flow)
                flow.response = http.Response.make(200, b'{"metadata": {}}')
                addon.response(flow)
                response = json.loads(flow.response.text)
                if mode in ("omitted", "removed_config"):
                    self.assertNotIn("storage-credentials", response)
                    self.assertNotIn("config", response)
                elif mode == "config":
                    self.assertNotIn("storage-credentials", response)
                    self.assertEqual(response["config"]["s3.access-key-id"], NEW_INIT_KEY)
                elif mode == "merged":
                    self.assertEqual(
                        response["storage-credentials"][0]["config"],
                        {"s3.access-key-id": NEW_INIT_KEY, "client.region": "us-east-1"},
                    )
                    self.assertEqual(response["config"]["s3.secret-access-key"], f"{NEW_INIT_KEY}_SECRET")
                    self.assertEqual(response["config"]["s3.region"], "wrong-region")
                elif mode == "invalid":
                    self.assertIsInstance(response["storage-credentials"], str)
                else:
                    credentials = response["storage-credentials"]
                    self.assertEqual(len(credentials), {"single": 1, "multiple": 2, "empty": 0}[mode])
                    if credentials:
                        self.assertEqual(credentials[-1]["config"]["s3.access-key-id"], NEW_INIT_KEY)
                    if mode == "multiple":
                        self.assertNotEqual(credentials[0]["prefix"], credentials[1]["prefix"])

    def test_config_errors_are_preserved(self):
        addon = VendedCredentialRefreshAddon()
        flow = catalog_flow("/v1/config", "omitted")
        addon.request(flow)
        flow.response = http.Response.make(403, b'{"error": {"message": "config denied"}}')
        addon.response(flow)
        self.assertEqual(flow.response.status_code, 403)
        self.assertEqual(json.loads(flow.response.text), {"error": {"message": "config denied"}})


if __name__ == "__main__":
    unittest.main()
