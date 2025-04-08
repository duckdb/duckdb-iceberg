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

DATA_GENERATION_DIR = f"./data/generated/iceberg/spark-rest/"
SCRIPT_DIR = f"./scripts/data_generators/"
INTERMEDIATE_DATA = "./data/generated/intermediates/spark-rest/"
PARQUET_SRC_FILE = f"scripts/data_generators/tmp_data/tmp.parquet"


class IcebergSparkRest:
    def __init__(self):
        pass

    ###
    ### Configure everyone's favorite apache product
    ###
    def GetConnection(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2 pyspark-shell"
        )
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ACCESS_KEY_ID"] = "admin"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

        spark = (
            SparkSession.builder.appName("DuckDB REST Integeration test")
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
            .config("spark.sql.defaultCatalog", "demo")
            .config('spark.driver.memory', '10g')
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config('spark.jars', f'{SCRIPT_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.2.jar')
            .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
            .getOrCreate()
        )
        spark.sql("CREATE DATABASE IF NOT EXISTS default;")
        return spark

    def GetSQLFiles(self, table_dir):
        sql_files = [f for f in os.listdir(table_dir) if f.endswith('.sql')]  # Find .sql files
        sql_files.sort()  # Order matters obviously # Store results
        return sql_files

    def GetTableDirs(self):
        dir = "./scripts/data_generators/generate_spark_rest/"
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
                CREATE or REPLACE TABLE default.{tbl}_sf1
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
        # con is spark_session
        # first get the sub_directories in the current directory
        for table_dir in self.GetTableDirs():
            full_table_dir = f"./scripts/data_generators/generate_spark_rest/{table_dir}"
            setup_script = self.GetSetupFile(full_table_dir)

            # should mimic generate_base_parquet
            if setup_script != "":
                os.system(
                    f"PARQUET_SRC_FILE='{PARQUET_SRC_FILE}' python3 {full_table_dir}/{os.path.basename(setup_script)}"
                )
                con.read.parquet(PARQUET_SRC_FILE).createOrReplaceTempView('parquet_file_view')

            update_files = self.GetSQLFiles(full_table_dir)

            for path in update_files:
                full_file_path = f"{full_table_dir}/{os.path.basename(path)}"
                with open(full_file_path, 'r') as file:
                    file_trimmed = os.path.basename(path)[:-4]
                    last_file = file_trimmed
                    query = file.read()
                    # Run spark query
                    con.sql(query)

                    # Create a parquet copy of table
                    df = con.read.table(f"default.{table_dir}")
                    df.write.mode("overwrite").parquet(f"{INTERMEDIATE_DATA}/{table_dir}/{file_trimmed}/data.parquet")

            ### Finally, copy the latest results to a "final" dir for easy test writing
            shutil.copytree(
                f"{INTERMEDIATE_DATA}/{table_dir}/{last_file}/data.parquet",
                f"{INTERMEDIATE_DATA}/{table_dir}/last/data.parquet",
                dirs_exist_ok=True,
            )

    def CloseConnection(self, con):
        del con
