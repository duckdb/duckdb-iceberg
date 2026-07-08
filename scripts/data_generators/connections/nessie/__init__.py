#!/usr/bin/python3

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os

import pyspark
import pyspark.sql
from pyspark import SparkContext
from pyspark.sql import SparkSession

from ..base import IcebergConnection
from scripts.data_generators.integration_config import get_spark_runtime


CONNECTION_KEY = "nessie"


@IcebergConnection.register(CONNECTION_KEY)
class IcebergSparkRest(IcebergConnection):
    def __init__(self, runtime=None):
        super().__init__(CONNECTION_KEY, "iceberg_catalog")
        self.runtime = get_spark_runtime(runtime)
        self.con = self.get_connection()

    def get_connection(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--packages {self.runtime.runtime_package},{self.runtime.aws_bundle_package} pyspark-shell"
        )
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("S3_KEY_ID", "minioadmin")
        os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("S3_SECRET", "minioadmin")
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()

        spark = (
            SparkSession.builder.appName("DuckDB REST Integration test")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg_catalog.type", "rest")
            .config("spark.sql.catalog.iceberg_catalog.uri", os.getenv("ICEBERG_ENDPOINT", "http://127.0.0.1:19120/iceberg/main/"))
            .config("spark.sql.catalog.iceberg_catalog.warehouse", os.getenv("WAREHOUSE", "warehouse"))
            .config("spark.sql.catalog.iceberg_catalog.s3.endpoint", os.getenv("S3_ENDPOINT", "http://127.0.0.1:9002"))
            .config("spark.sql.catalog.iceberg_catalog.s3.path-style-access", "true")
            .config("spark.driver.memory", "10g")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.jars", self.runtime.jar_path.as_posix())
            .config("spark.sql.catalog.iceberg_catalog.scope", "catalog sign")
            .config(
                "spark.sql.catalog.iceberg_catalog.oauth2-server-uri",
                os.getenv("OAUTH2_SERVER_URI", "http://127.0.0.1:8080/realms/iceberg/protocol/openid-connect/token"),
            )
            .config(
                "spark.sql.catalog.iceberg_catalog.credential",
                f"{os.getenv('ICEBERG_CLIENT_ID', 'client1')}:{os.getenv('ICEBERG_CLIENT_SECRET', 's3cr3t')}",
            )
            .config("spark.sql.catalog.iceberg_catalog.rest.auth.type", "oauth2")
            .getOrCreate()
        )
        try:
            jvm = spark.sparkContext._jvm
            logger = jvm.org.apache.log4j.LogManager.getLogger("org.apache.iceberg.aws.s3.S3FileIO")
            logger.setLevel(jvm.org.apache.log4j.Level.ERROR)
        except Exception:
            pass
        spark.sql("USE iceberg_catalog")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        return spark
