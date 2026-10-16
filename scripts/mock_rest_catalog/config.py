"""Read the mock endpoint from the canonical unittest initialization SQL."""

import json
import re
from pathlib import Path
from urllib.parse import urlsplit


CONFIG = Path(__file__).resolve().parents[2] / "test/configs/mock.json"


def load_config():
    config = json.loads(CONFIG.read_text())
    match = re.search(r"\bURI\s+'([^']+)'", config["on_init"], re.IGNORECASE)
    if not match:
        raise ValueError(f"Missing URI in {CONFIG}")
    uri = match[1]
    address = urlsplit(uri)
    if (
        address.scheme != "http"
        or address.hostname != "127.0.0.1"
        or not address.port
        or address.path
        or address.query
        or address.fragment
        or address.username
    ):
        raise ValueError(f"Mock URI must be http://127.0.0.1:<port>: {uri}")
    return config, uri
