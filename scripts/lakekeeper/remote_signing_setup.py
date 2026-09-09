"""Create the Lakekeeper warehouse the remote signing tests read from.

Runs inside the Lakekeeper example 'jupyter' container, so all service names are the ones
of the docker compose network. The warehouse disables STS, which makes Lakekeeper answer a
'remote-signing' access delegation request with signer information instead of credentials.
"""

import pandas as pd
import pyspark
import requests
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

MANAGEMENT_URL = "http://lakekeeper:8181/management"
CATALOG_URL = "http://lakekeeper:8181/catalog"
KEYCLOAK_TOKEN_URL = "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"

WAREHOUSE = "demo_remote_signing"
NAMESPACE = "remote_signing"
TABLE = "remote_signing_table"

CLIENT_ID = "spark"
CLIENT_SECRET = "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"
ICEBERG_VERSION = "1.10.0"


def get_access_token() -> str:
    response = requests.post(
        url=KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "lakekeeper",
        },
        headers={"Content-type": "application/x-www-form-urlencoded"},
    )
    response.raise_for_status()
    return response.json()["access_token"]


def create_warehouse(access_token: str) -> None:
    response = requests.post(
        url=f"{MANAGEMENT_URL}/v1/warehouse",
        headers={"Authorization": f"Bearer {access_token}"},
        json={
            "warehouse-name": WAREHOUSE,
            "storage-profile": {
                "type": "s3",
                "bucket": "examples",
                "key-prefix": "remote-signing-warehouse",
                "endpoint": "http://minio:9000",
                "region": "local-01",
                "path-style-access": True,
                "flavor": "s3-compat",
                "sts-enabled": False,
                "remote-signing-enabled": True,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": "minio-root-user",
                "aws-secret-access-key": "minio-root-password",
            },
        },
    )
    if response.status_code == 409:
        print(f"Warehouse '{WAREHOUSE}' already exists")
        return
    response.raise_for_status()
    print(f"Created warehouse '{WAREHOUSE}'")


def seed_table() -> None:
    spark_minor_version = ".".join(pyspark.__version__.split(".")[:2])
    conf = {
        "spark.jars.packages": (
            f"org.apache.iceberg:iceberg-spark-runtime-{spark_minor_version}_2.12:{ICEBERG_VERSION},"
            f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"
        ),
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lakekeeper.type": "rest",
        "spark.sql.catalog.lakekeeper.uri": CATALOG_URL,
        "spark.sql.catalog.lakekeeper.credential": f"{CLIENT_ID}:{CLIENT_SECRET}",
        "spark.sql.catalog.lakekeeper.warehouse": WAREHOUSE,
        "spark.sql.catalog.lakekeeper.scope": "lakekeeper",
        "spark.sql.catalog.lakekeeper.oauth2-server-uri": KEYCLOAK_TOKEN_URL,
        "spark.sql.catalog.lakekeeper.header.X-Iceberg-Access-Delegation": "remote-signing",
    }

    spark_config = SparkConf().setMaster("local").setAppName("Iceberg-REST-Remote-Signing")
    for key, value in conf.items():
        spark_config = spark_config.set(key, value)
    spark = SparkSession.builder.config(conf=spark_config).getOrCreate()

    spark.sql("USE lakekeeper")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    data = pd.DataFrame(
        [[1, "a-string", 2.2], [2, "another-string", 3.3], [3, "a-third-string", 4.4]],
        columns=["id", "strings", "floats"],
    )
    spark.createDataFrame(data).writeTo(f"{NAMESPACE}.{TABLE}").createOrReplace()
    print(f"Seeded '{WAREHOUSE}.{NAMESPACE}.{TABLE}'")
    spark.stop()


if __name__ == "__main__":
    create_warehouse(get_access_token())
    seed_table()
