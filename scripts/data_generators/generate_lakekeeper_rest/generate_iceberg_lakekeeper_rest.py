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

#!/usr/bin/python3
import pyspark
import pyspark.sql
import sys
import duckdb
import os
from pyspark import SparkContext
from pathlib import Path
import shutil

PARQUET_SRC_FILE = f"scripts/data_generators/tmp_data/tmp.parquet"

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

CATALOG_URL = "http://localhost:8181/catalog"
MANAGEMENT_URL = "http://localhost:8181/management"
KEYCLOAK_TOKEN_URL = "http://localhost:30080/realms/iceberg/protocol/openid-connect/token"
WAREHOUSE = "demo"

CLIENT_ID = "spark"
CLIENT_SECRET = "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"

SPARK_VERSION = pyspark.__version__
SPARK_MINOR_VERSION = '.'.join(SPARK_VERSION.split('.')[:2])
ICEBERG_VERSION = "1.7.0"


class IcebergLakekeeperRest:
    def __init__(self):
        pass

    ###
    ### Configure everyone's favorite apache product
    ###
    def GetConnection(self):

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
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        spark.sql("USE NAMESPACE default")
        return spark

    def GetSQLFiles(self, table_dir):
        sql_files = [f for f in os.listdir(table_dir) if f.endswith('.sql')]  # Find .sql files
        sql_files.sort()  # Order matters obviously # Store results
        return sql_files

    def GetTableDirs(self):
        dir = "./scripts/data_generators/generate_lakekeeper_rest/"
        subdirectories = [d for d in os.listdir(dir) if os.path.isdir(dir + d) and d != "__pycache__"]
        return subdirectories

    def GetSetupFile(self, dir):
        setup_files = [f for f in os.listdir(dir) if 'setup' in f.lower()]
        if len(setup_files) == 0:
            return ""
        return setup_files[0]

    def GenerateTPCH(self, con):
        duckdb_con = duckdb.connect()
        duckdb_con.execute("call dbgen(sf=1)")

        for tbl in ['lineitem', 'customer', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']:
            create_statement = f"""
                CREATE or REPLACE TABLE default.{tbl}
                TBLPROPERTIES (
                    'format-version'='2',
                    'write.update.mode'='merge-on-read'
                )
                AS SELECT * FROM parquet_file_view;
            """
            duckdb_con.execute(f"copy {tbl} to '{PARQUET_SRC_FILE}' (FORMAT PARQUET)")
            con.read.parquet(PARQUET_SRC_FILE).createOrReplaceTempView('parquet_file_view')
            con.sql(create_statement)

    def GenerateTables(self, con):
        # Generate the tpch tables
        self.GenerateTPCH(con)
        con.sql("CREATE NAMESPACE IF NOT EXISTS COLLADO_TEST")
        con.sql("USE NAMESPACE COLLADO_TEST")
        con.sql(
            """
        CREATE TABLE IF NOT EXISTS quickstart_table (
          id BIGINT, data STRING
        )
        USING ICEBERG
        """
        )
        con.sql("INSERT INTO quickstart_table VALUES (1, 'some data'), (2, 'more data'), (3, 'yet more data')")

    def CloseConnection(self, con):
        del con
