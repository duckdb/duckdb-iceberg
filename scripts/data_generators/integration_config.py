from __future__ import annotations

import json
import os
import importlib
import importlib.util
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from packaging.version import Version


REPO_ROOT = Path(__file__).resolve().parents[2]
TEST_CONFIG_DIR = REPO_ROOT / "test" / "configs"
DATA_GENERATORS_DIR = REPO_ROOT / "scripts" / "data_generators"
ACTIVE_CATALOG_FILE = REPO_ROOT / ".catalogs" / ".active_catalog"

REST_CATALOG_NAMES = ("fixture", "fixture-latest", "gravitino", "lakekeeper", "polaris", "nessie")
GENERATOR_CATALOG_NAMES = ("fixture", "fixture-latest", "gravitino", "lakekeeper", "local", "polaris", "nessie")


@dataclass(frozen=True)
class SparkRuntime:
    name: str
    spark_version: Version
    scala_binary_version: str
    iceberg_library_version: str
    supports_v3: bool

    @property
    def jar_filename(self) -> str:
        return (
            "iceberg-spark-runtime-"
            f"{self.spark_version}_{self.scala_binary_version}-{self.iceberg_library_version}.jar"
        )

    @property
    def jar_path(self) -> Path:
        return DATA_GENERATORS_DIR / self.jar_filename

    @property
    def runtime_package(self) -> str:
        return (
            "org.apache.iceberg:"
            f"iceberg-spark-runtime-{self.spark_version}_{self.scala_binary_version}:{self.iceberg_library_version}"
        )

    @property
    def aws_bundle_package(self) -> str:
        return f"org.apache.iceberg:iceberg-aws-bundle:{self.iceberg_library_version}"

    def matches_pyspark(self, pyspark_version: Version) -> bool:
        return tuple(pyspark_version.release[:2]) == tuple(self.spark_version.release[:2])

    @property
    def capabilities(self) -> frozenset[str]:
        capabilities = set()
        if self.supports_v3:
            capabilities.add("format_v3")
        return frozenset(capabilities)


@dataclass(frozen=True)
class CatalogProfile:
    name: str
    connection_key: str
    unittest_config: Path
    pyiceberg_uri: str
    pyiceberg_warehouse: str | None
    pyiceberg_options: dict[str, object]
    pyiceberg_oauth_token_url: str | None = None
    pyiceberg_oauth_payload: dict[str, str] | None = None
    supports_v3_tables: bool = True
    supports_row_lineage: bool = True

    @property
    def duckdb_catalog_init_sql(self) -> str:
        return load_test_config(self.unittest_config)["on_init"]

    def build_pyiceberg_config(self) -> dict[str, object]:
        config: dict[str, object] = {
            "uri": self.pyiceberg_uri,
            **self.pyiceberg_options,
        }
        if self.pyiceberg_warehouse:
            config["warehouse"] = self.pyiceberg_warehouse

        if self.pyiceberg_oauth_payload is not None:
            credential = self.pyiceberg_oauth_payload["client_secret"]
            if client_id := self.pyiceberg_oauth_payload.get("client_id"):
                credential = f"{client_id}:{credential}"
            config["credential"] = credential
            config["scope"] = self.pyiceberg_oauth_payload.get("scope", "catalog")
            if self.pyiceberg_oauth_token_url:
                config["oauth2-server-uri"] = self.pyiceberg_oauth_token_url

        return config

    @property
    def capabilities(self) -> frozenset[str]:
        capabilities = set()
        if self.supports_v3_tables:
            capabilities.add("format_v3")
        if self.supports_row_lineage:
            capabilities.add("row_lineage")
        return frozenset(capabilities)


SPARK_RUNTIMES = {
    "3.5": SparkRuntime(
        name="3.5",
        spark_version=Version("3.5"),
        scala_binary_version="2.12",
        iceberg_library_version="1.9.0",
        supports_v3=False,
    ),
    "4.0": SparkRuntime(
        name="4.0",
        spark_version=Version("4.0"),
        scala_binary_version="2.13",
        iceberg_library_version="1.10.0",
        supports_v3=True,
    ),
    "4.1": SparkRuntime(
        name="4.1",
        spark_version=Version("4.1"),
        scala_binary_version="2.13",
        iceberg_library_version="1.11.0",
        supports_v3=True,
    ),
}

REST_CATALOG_PROFILES = {
    "fixture": CatalogProfile(
        name="fixture",
        connection_key="fixture",
        unittest_config=TEST_CONFIG_DIR / "fixture.json",
        pyiceberg_uri=os.getenv("ICEBERG_ENDPOINT", "http://127.0.0.1:8181"),
        pyiceberg_warehouse=os.getenv("WAREHOUSE", ""),
        pyiceberg_oauth_token_url=f"{os.getenv('ICEBERG_ENDPOINT', 'http://127.0.0.1:8181')}/v1/oauth/tokens",
        pyiceberg_oauth_payload={
            "grant_type": "client_credentials",
            "client_id": os.getenv("ICEBERG_CLIENT_ID", "admin"),
            "client_secret": os.getenv("ICEBERG_CLIENT_SECRET", "password"),
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        pyiceberg_options={
            "s3.endpoint": os.getenv("S3_ENDPOINT", "http://127.0.0.1:9000"),
            "s3.access-key-id": os.getenv("S3_KEY_ID", "admin"),
            "s3.secret-access-key": os.getenv("S3_SECRET", "password"),
            "s3.path-style-access": "true",
            "s3.ssl.enabled": "false",
        },
    ),
    "fixture-latest": CatalogProfile(
        name="fixture-latest",
        connection_key="fixture",
        unittest_config=TEST_CONFIG_DIR / "fixture-latest.json",
        pyiceberg_uri=os.getenv("ICEBERG_ENDPOINT", "http://127.0.0.1:8181"),
        pyiceberg_warehouse=os.getenv("WAREHOUSE", ""),
        pyiceberg_oauth_token_url=f"{os.getenv('ICEBERG_ENDPOINT', 'http://127.0.0.1:8181')}/v1/oauth/tokens",
        pyiceberg_oauth_payload={
            "grant_type": "client_credentials",
            "client_id": os.getenv("ICEBERG_CLIENT_ID", "admin"),
            "client_secret": os.getenv("ICEBERG_CLIENT_SECRET", "password"),
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        pyiceberg_options={
            "s3.endpoint": os.getenv("S3_ENDPOINT", "http://127.0.0.1:9000"),
            "s3.access-key-id": os.getenv("S3_KEY_ID", "admin"),
            "s3.secret-access-key": os.getenv("S3_SECRET", "password"),
            "s3.path-style-access": "true",
            "s3.ssl.enabled": "false",
        },
    ),
    "gravitino": CatalogProfile(
        name="gravitino",
        connection_key="gravitino",
        unittest_config=TEST_CONFIG_DIR / "gravitino.json",
        pyiceberg_uri=os.getenv("ICEBERG_ENDPOINT", "http://127.0.0.1:9001/iceberg"),
        pyiceberg_warehouse=None,
        pyiceberg_options={
            "auth": {"type": "noop"},
            "s3.endpoint": os.getenv("S3_ENDPOINT", "http://127.0.0.1:9000"),
            "s3.access-key-id": os.getenv("S3_KEY_ID", "admin"),
            "s3.secret-access-key": os.getenv("S3_SECRET", "password"),
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    ),
    "lakekeeper": CatalogProfile(
        name="lakekeeper",
        connection_key="lakekeeper",
        unittest_config=TEST_CONFIG_DIR / "lakekeeper.json",
        pyiceberg_uri=os.getenv("ICEBERG_ENDPOINT", "http://localhost:8181/catalog"),
        pyiceberg_warehouse=os.getenv("WAREHOUSE", "demo"),
        pyiceberg_oauth_token_url=os.getenv(
            "OAUTH2_SERVER_URI",
            "http://localhost:30080/realms/iceberg/protocol/openid-connect/token",
        ),
        pyiceberg_oauth_payload={
            "grant_type": "client_credentials",
            "client_id": os.getenv("ICEBERG_CLIENT_ID", "spark"),
            "client_secret": os.getenv("ICEBERG_CLIENT_SECRET", "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"),
            "scope": os.getenv("OAUTH2_SCOPE", "lakekeeper"),
        },
        pyiceberg_options={},
    ),
    "polaris": CatalogProfile(
        name="polaris",
        connection_key="polaris",
        unittest_config=TEST_CONFIG_DIR / "polaris.json",
        pyiceberg_uri=os.getenv("ICEBERG_ENDPOINT", "http://localhost:8181/api/catalog"),
        pyiceberg_warehouse=os.getenv("WAREHOUSE", "quickstart_catalog"),
        pyiceberg_oauth_token_url=os.getenv("OAUTH2_SERVER_URI", "http://localhost:8181/api/catalog/v1/oauth/tokens"),
        pyiceberg_oauth_payload={
            "grant_type": "client_credentials",
            "client_id": "root",
            "client_secret": "s3cr3t",
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        pyiceberg_options={
            "header.X-Iceberg-Access-Delegation": "vended-credentials",
            "s3.region": "us-west-2",
        },
    ),
    "nessie": CatalogProfile(
        name="nessie",
        connection_key="nessie",
        unittest_config=TEST_CONFIG_DIR / "nessie.json",
        pyiceberg_uri=os.getenv("ICEBERG_ENDPOINT", "http://127.0.0.1:19120/iceberg/main/"),
        pyiceberg_warehouse=os.getenv("WAREHOUSE", "warehouse"),
        pyiceberg_oauth_token_url=os.getenv(
            "OAUTH2_SERVER_URI",
            "http://127.0.0.1:8080/realms/iceberg/protocol/openid-connect/token",
        ),
        pyiceberg_oauth_payload={
            "grant_type": "client_credentials",
            "client_id": os.getenv("ICEBERG_CLIENT_ID", "client1"),
            "client_secret": os.getenv("ICEBERG_CLIENT_SECRET", "s3cr3t"),
            "scope": os.getenv("OAUTH2_SCOPE", "catalog sign"),
        },
        pyiceberg_options={
            "s3.endpoint": os.getenv("S3_ENDPOINT", "http://127.0.0.1:9002"),
            "s3.access-key-id": os.getenv("S3_KEY_ID", "minioadmin"),
            "s3.secret-access-key": os.getenv("S3_SECRET", "minioadmin"),
            "s3.path-style-access": "true",
            "s3.ssl.enabled": "false",
        },
        supports_v3_tables=False,
        supports_row_lineage=False,
    ),
}


def get_spark_runtime(runtime: str | SparkRuntime) -> SparkRuntime:
    if isinstance(runtime, SparkRuntime):
        return runtime

    try:
        return SPARK_RUNTIMES[runtime]
    except KeyError as exc:
        raise KeyError(f"Unknown spark runtime '{runtime}'") from exc


def get_rest_catalog_profile(catalog: str) -> CatalogProfile:
    try:
        return REST_CATALOG_PROFILES[catalog]
    except KeyError as exc:
        raise KeyError(f"Unknown REST catalog '{catalog}'") from exc


def resolve_active_catalog(*, allowed_catalogs: tuple[str, ...], purpose: str) -> str:
    if not ACTIVE_CATALOG_FILE.exists():
        raise RuntimeError(
            f"Cannot determine the active catalog for {purpose}: "
            f"'{ACTIVE_CATALOG_FILE}' does not exist. Start a catalog via the make targets first."
        )

    active_catalog = ACTIVE_CATALOG_FILE.read_text(encoding="utf-8").strip()
    if not active_catalog:
        raise RuntimeError(
            f"Cannot determine the active catalog for {purpose}: "
            f"'{ACTIVE_CATALOG_FILE}' is empty."
        )

    if active_catalog not in allowed_catalogs:
        allowed = ", ".join(allowed_catalogs)
        raise RuntimeError(
            f"Active catalog '{active_catalog}' is not supported for {purpose}. "
            f"Expected one of: {allowed}."
        )

    return active_catalog


def resolve_pyspark_runtime(*, purpose: str) -> tuple[SparkRuntime, Version]:
    if importlib.util.find_spec("pyspark") is None:
        raise RuntimeError(
            f"PySpark is required for {purpose}, but it is not installed in the current environment."
        )

    pyspark = importlib.import_module("pyspark")
    pyspark_version = Version(pyspark.__version__)
    version_key = ".".join(str(part) for part in pyspark_version.release[:2])
    runtime = SPARK_RUNTIMES.get(version_key)
    if runtime is None:
        supported = ", ".join(SPARK_RUNTIMES.keys())
        raise RuntimeError(
            f"PySpark {pyspark_version} is not supported for {purpose}. "
            f"Supported PySpark versions map to runtimes: {supported}."
        )
    return runtime, pyspark_version


@lru_cache(maxsize=None)
def load_test_config(config_path: Path | str) -> dict[str, object]:
    with Path(config_path).open("r", encoding="utf-8") as file:
        return json.load(file)
