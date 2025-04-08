#!/usr/bin/python3
import pyspark
import pyspark.sql
import sys
import os
from pyspark import SparkContext
from pathlib import Path
import shutil


# import DataGenerationBase

# from scripts.data_generators.generate_base_parquet import PARQUET_SRC_FILE

DATA_GENERATION_DIR = f"./data/generated/iceberg/spark-local/"
SCRIPT_DIR = f"./scripts/data_generators/"
INTERMEDIATE_DATA = "./data/generated/intermediates/spark-local/"


class IcebergSparkLocal:
    def __init__(self):
        pass

    ###
    ### Configure everyone's favorite apache product
    ###
    def GetConnection(self):
        conf = pyspark.SparkConf()
        conf.setMaster('local[*]')
        conf.set('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog')
        conf.set('spark.sql.catalog.iceberg_catalog.type', 'hadoop')
        conf.set('spark.sql.catalog.iceberg_catalog.warehouse', DATA_GENERATION_DIR)
        conf.set('spark.sql.parquet.outputTimestampType', 'TIMESTAMP_MICROS')
        conf.set('spark.driver.memory', '10g')
        conf.set('spark.jars', f'{SCRIPT_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.2.jar')
        conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
        sc = spark.sparkContext
        sc.setLogLevel("ERROR")
        return spark

    def GetSQLFiles(self, table_dir):
        sql_files = [f for f in os.listdir(table_dir) if f.endswith('.sql')]  # Find .sql files
        sql_files.sort()  # Order matters obviously # Store results
        return sql_files

    def GetTableDirs(self):
        dir = "./scripts/data_generators/generate_spark_local/"
        subdirectories = [d for d in os.listdir(dir) if os.path.isdir(dir + d) and d != "__pycache__"]
        return subdirectories

    def GetSetupFile(self, dir):
        setup_files = [f for f in os.listdir(dir) if 'setup' in f.lower()]
        if len(setup_files) == 0:
            return ""
        return setup_files[0]

    def GenerateTables(self, con):
        # con is spark_session
        # first get the sub_directories in the current directory
        for table_dir in self.GetTableDirs():
            full_table_dir = f"./scripts/data_generators/generate_spark_local/{table_dir}"
            setup_script = self.GetSetupFile(full_table_dir)

            PARQUET_SRC_FILE = f"scripts/data_generators/tmp_data/tmp.parquet"
            if setup_script != "":
                os.system(
                    f"PARQUET_SRC_FILE='{PARQUET_SRC_FILE}' python3 {full_table_dir}/{os.path.basename(setup_script)}"
                )
                con.read.parquet(PARQUET_SRC_FILE).createOrReplaceTempView('parquet_file_view')

            update_files = self.GetSQLFiles(full_table_dir)

            last_file = ""
            for path in update_files:
                full_file_path = f"{full_table_dir}/{os.path.basename(path)}"
                with open(full_file_path, 'r') as file:
                    file_trimmed = os.path.basename(path)[:-4]
                    last_file = file_trimmed
                    query = file.read()
                    # Run spark query
                    con.sql(query)

                    # Create a parquet copy of table
                    df = con.read.table(f"iceberg_catalog.{table_dir}")
                    df.write.mode("overwrite").parquet(f"{INTERMEDIATE_DATA}/{table_dir}/{file_trimmed}/data.parquet")

            if last_file != "":
                ### Finally, copy the latest results to a "final" dir for easy test writing
                shutil.copytree(
                    f"{INTERMEDIATE_DATA}/{table_dir}/{last_file}/data.parquet",
                    f"{INTERMEDIATE_DATA}/{table_dir}/last/data.parquet",
                    dirs_exist_ok=True,
                )

    def CloseConnection(self, con):
        pass
