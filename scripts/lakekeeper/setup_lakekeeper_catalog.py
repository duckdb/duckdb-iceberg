import requests

import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pandas as pd

CATALOG_URL = "http://localhost:8181/catalog"
MANAGEMENT_URL = "http://localhost:8181/management"
KEYCLOAK_TOKEN_URL = "http://localhost:30080/realms/iceberg/protocol/openid-connect/token"
WAREHOUSE = "demo"

CLIENT_ID = "spark"
CLIENT_SECRET = "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"

SPARK_VERSION = pyspark.__version__
SPARK_MINOR_VERSION = '.'.join(SPARK_VERSION.split('.')[:2])
ICEBERG_VERSION = "1.7.0"

# Get an access token from keycloak (the idP server)
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
access_token = response.json()['access_token']
print(access_token)

# Check the 'info' endpoint of the Lakekeeper management API
response = requests.get(
    url=f"{MANAGEMENT_URL}/v1/info",
    headers={"Authorization": f"Bearer {access_token}"},
)
response.raise_for_status()
print(response.json())

# Bootstrap
response = requests.post(
    url=f"{MANAGEMENT_URL}/v1/bootstrap",
    headers={"Authorization": f"Bearer {access_token}"},
    json={
        "accept-terms-of-use": True,
    },
)
response.raise_for_status()

# Create a warehouse
response = requests.post(
    url=f"{MANAGEMENT_URL}/v1/warehouse",
    headers={"Authorization": f"Bearer {access_token}"},
    json={
        "warehouse-name": WAREHOUSE,
        "storage-profile": {
            "type": "s3",
            "bucket": "examples",
            "key-prefix": "initial-warehouse",
            "endpoint": "http://localhost:9000",
            "region": "local-01",
            "path-style-access": True,
            "flavor": "minio",
            "sts-enabled": True,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": "minio-root-user",
            "aws-secret-access-key": "minio-root-password",
        },
    },
)
response.raise_for_status()
print(response.json())

# Populate the warehouse with Spark
conf = {
    "spark.jars.packages": f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR_VERSION}_2.12:{ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakekeeper.type": "rest",
    "spark.sql.catalog.lakekeeper.uri": CATALOG_URL,
    "spark.sql.catalog.lakekeeper.credential": f"{CLIENT_ID}:{CLIENT_SECRET}",
    "spark.sql.catalog.lakekeeper.warehouse": WAREHOUSE,
    "spark.sql.catalog.lakekeeper.scope": "lakekeeper",
    "spark.sql.catalog.lakekeeper.oauth2-server-uri": KEYCLOAK_TOKEN_URL,
}

spark_config = SparkConf().setMaster('local').setAppName("Iceberg-REST")
for k, v in conf.items():
    spark_config = spark_config.set(k, v)

spark = SparkSession.builder.config(conf=spark_config).getOrCreate()

spark.sql("USE lakekeeper")

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS my_namespace")
data = pd.DataFrame([[1, 'a-string', 2.2]], columns=['id', 'strings', 'floats'])
sdf = spark.createDataFrame(data)
sdf.writeTo(f"my_namespace.my_table").createOrReplace()
