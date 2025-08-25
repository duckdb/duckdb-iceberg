#!/usr/bin/python3
import os
from typing import Callable, Dict, Type, List
from dataclasses import dataclass
from scripts.data_generators.connections import IcebergConnection

from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark
import pyspark.sql

import sys
import duckdb
from pathlib import Path
import shutil

SCRIPT_DIR = os.path.dirname(__file__)
INTERMEDIATE_DIR = os.path.join(SCRIPT_DIR, '..', '..', '..', 'data', 'generated', 'intermediates')


class IcebergTest:
    registry: List[Type['IcebergTest']] = []

    @classmethod
    def register(cls):
        def decorator(subclass):
            cls.registry.append(subclass)
            return subclass

        return decorator

    def __init__(self, table: str, *, namespace=None, write_intermediates=True):
        self.table = table
        self.write_intermediates = write_intermediates
        self.namespace = ['default'] if not namespace else namespace
        self.files = self.get_files()

    def get_files(self):
        sql_files = [f for f in os.listdir(os.path.join(SCRIPT_DIR, self.table)) if f.endswith('.sql')]
        sql_files.sort()
        return sql_files

    def generate(self, con: IcebergConnection):
        self.setup(con)

        intermediate_dir = os.path.join(INTERMEDIATE_DIR, con.name, self.table)
        last_file = None
        for path in self.files:
            full_file_path = os.path.join(SCRIPT_DIR, self.table, path)
            with open(full_file_path, 'r') as file:
                snapshot_name = os.path.basename(path)[:-4]
                last_file = snapshot_name
                query = file.read()
                try:
                    # Run spark query
                    con.con.sql(query)
                except Exception as e:
                    print(f"Error executing query from {path}: {e}")
                    return  # Exit gracefully

                if self.write_intermediates:
                    # Create a parquet copy of table
                    namespace = '.'.join(self.namespace)
                    df = con.con.read.table(f"{namespace}.{self.table}")
                    intermediate_data_path = os.path.join(intermediate_dir, snapshot_name, 'data.parquet')
                    df.write.mode("overwrite").parquet(intermediate_data_path)

        if self.write_intermediates:
            ### Finally, copy the latest results to a "last" dir for easy test writing
            shutil.copytree(
                os.path.join(intermediate_dir, last_file, 'data.parquet'),
                os.path.join(intermediate_dir, 'last', 'data.parquet'),
                dirs_exist_ok=True,
            )

    def setup(self, con: IcebergConnection):
        pass
