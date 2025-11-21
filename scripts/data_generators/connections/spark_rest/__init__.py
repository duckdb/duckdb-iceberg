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

from pyspark.sql import SparkSession
import pyspark
import pyspark.sql
from pyspark import SparkContext

from ..base import IcebergConnection

import sys
import os

CONNECTION_KEY = 'spark-rest'
SPARK_RUNTIME_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'iceberg-spark-runtime-3.5_2.12-1.9.0.jar')


@IcebergConnection.register(CONNECTION_KEY)
class IcebergSparkRest(IcebergConnection):
    def __init__(self):
        super().__init__(CONNECTION_KEY, 'demo')
        self.con = self.get_connection()

    def get_connection(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0,org.apache.iceberg:iceberg-aws-bundle:1.9.0 pyspark-shell"
        )
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ACCESS_KEY_ID"] = "admin"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

        spark = (
            SparkSession.builder.appName("DuckDB REST Integration test")
            .master("local[1]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.demo.type", "rest")
            .config("spark.sql.catalog.demo.uri", "http://127.0.0.1:8181")
            .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/wh/")
            .config("spark.sql.catalog.demo.s3.endpoint", "http://127.0.0.1:9000")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config('spark.driver.memory', '10g')
            .config('spark.sql.session.timeZone', 'UTC')
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config('spark.jars', SPARK_RUNTIME_PATH)
            .getOrCreate()
        )
        spark.sql("USE demo")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        return spark
