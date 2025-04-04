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
import duckdb
import shutil

DATA_GENERATION_DIR = f"./data/generated/iceberg/polaris-rest/"
SCRIPT_DIR = f"./scripts/data_generators/"
INTERMEDIATE_DATA = "./data/generated/intermediates/polaris-rest/"
PARQUET_SRC_FILE = f"scripts/data_generators/tmp_data/tmp.parquet"


class IcebergPolarisRest:
    def __init__(self):
        pass

    ###
    ### Configure everyone's favorite apache product
    ###
    def GetConnection(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2 pyspark-shell"
        )

        client_id = os.getenv('POLARIS_CLIENT_ID', '')
        client_secret = os.getenv('POLARIS_CLIENT_SECRET', '')
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ACCESS_KEY_ID"] = "admin"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

        if client_id == '' or client_secret == '':
            print("could not find client id or client secret to connect to polaris, aborting")
            return

        spark = (
            SparkSession.builder.config(
                "spark.sql.catalog.quickstart_catalog", "org.apache.iceberg.spark.SparkSessionCatalog"
            )
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.hadoop:hadoop-aws:3.4.0,software.amazon.awssdk:bundle:2.23.19,software.amazon.awssdk:url-connection-client:2.23.19",
            )
            .config('spark.sql.iceberg.vectorization.enabled', 'false')
            # Configure the 'polaris' catalog as an Iceberg rest catalog
            .config("spark.sql.catalog.quickstart_catalog.type", "rest")
            .config("spark.sql.catalog.quickstart_catalog", "org.apache.iceberg.spark.SparkCatalog")
            # Specify the rest catalog endpoint
            .config("spark.sql.catalog.quickstart_catalog.uri", "http://localhost:8181/api/catalog")
            # Enable token refresh
            .config("spark.sql.catalog.quickstart_catalog.token-refresh-enabled", "true")
            # specify the client_id:client_secret pair
            .config("spark.sql.catalog.quickstart_catalog.credential", f"{client_id}:{client_secret}")
            # Set the warehouse to the name of the catalog we created
            .config("spark.sql.catalog.quickstart_catalog.warehouse", "quickstart_catalog")
            # Scope set to PRINCIPAL_ROLE:ALL
            .config("spark.sql.catalog.quickstart_catalog.scope", 'PRINCIPAL_ROLE:ALL')
            # Enable access credential delegation
            .config("spark.sql.catalog.quickstart_catalog.header.X-Iceberg-Access-Delegation", 'vended-credentials')
            .config("spark.sql.catalog.quickstart_catalog.io-impl", "org.apache.iceberg.io.ResolvingFileIO")
            .config("spark.sql.catalog.quickstart_catalog.s3.region", "us-west-2")
            .config("spark.history.fs.logDirectory", "/home/iceberg/spark-events")
        ).getOrCreate()
        spark.sql("USE quickstart_catalog")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        spark.sql("USE NAMESPACE default")
        return spark

    def GetSQLFiles(self, table_dir):
        sql_files = [f for f in os.listdir(table_dir) if f.endswith('.sql')]  # Find .sql files
        sql_files.sort()  # Order matters obviously # Store results
        return sql_files

    def GetTableDirs(self):
        dir = "./scripts/data_generators/generate_polaris_rest/"
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
