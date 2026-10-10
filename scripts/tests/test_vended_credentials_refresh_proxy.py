import json
import unittest
from types import SimpleNamespace

from mitmproxy import http

from scripts.vended_credentials_refresh_proxy import CREDENTIALS_ENDPOINT, VendedCredentialRefreshAddon


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


if __name__ == "__main__":
    unittest.main()
