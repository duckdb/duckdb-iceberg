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


CONNECTION_KEY = "fixture"


@IcebergConnection.register(CONNECTION_KEY)
@IcebergConnection.register(CONNECTION_KEY + '-latest')
class IcebergSparkRest(IcebergConnection):
    connection_key = CONNECTION_KEY
    default_endpoint = "http://127.0.0.1:8181"
    default_warehouse = "s3://warehouse/wh/"
    default_s3_endpoint = "http://127.0.0.1:9000"
    default_s3_key_id = "admin"
    default_s3_secret = "password"

    def __init__(self, runtime=None):
        super().__init__(self.connection_key, "demo")
        self.runtime = get_spark_runtime(runtime)
        self.con = self.get_connection()

    def get_connection(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--packages {self.runtime.runtime_package},{self.runtime.aws_bundle_package} pyspark-shell"
        )
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("S3_KEY_ID", self.default_s3_key_id)
        os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("S3_SECRET", self.default_s3_secret)
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()

        builder = (
            SparkSession.builder.appName("DuckDB REST Integration test")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.demo.type", "rest")
            .config("spark.sql.catalog.demo.uri", os.getenv("ICEBERG_ENDPOINT", self.default_endpoint))
            .config("spark.sql.catalog.demo.s3.endpoint", os.getenv("S3_ENDPOINT", self.default_s3_endpoint))
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config("spark.driver.memory", "10g")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.jars", self.runtime.jar_path.as_posix())
        )
        warehouse = os.getenv("WAREHOUSE", self.default_warehouse)
        if warehouse:
            builder = builder.config("spark.sql.catalog.demo.warehouse", warehouse)
        spark = builder.getOrCreate()
        try:
            jvm = spark.sparkContext._jvm
            logger = jvm.org.apache.log4j.LogManager.getLogger("org.apache.iceberg.aws.s3.S3FileIO")
            logger.setLevel(jvm.org.apache.log4j.Level.ERROR)
        except Exception:
            pass
        spark.sql("USE demo")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        return spark
