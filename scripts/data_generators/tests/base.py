#!/usr/bin/python3
import os
from typing import Type, List, Optional
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

    # Default mapping from high-level catalog names to connection registry keys.
    # Extend this map as you introduce new catalogs/targets.
    CATALOG_DEFAULT_TARGET = {
        "polaris": "polaris",
        "lakekeeper": "lakekeeper",
        "spark-rest": "spark-rest",
        "local": "local",
        "nessie": "nessie",
    }

    def resolve_target_for_catalog(self, catalog: str, target: Optional[str] = None) -> str:
        if target:
            return target
        if catalog in self.CATALOG_DEFAULT_TARGET:
            return self.CATALOG_DEFAULT_TARGET[catalog]
        raise ValueError(
            f"No default target mapping for catalog '{catalog}'. Supply an explicit target or extend CATALOG_DEFAULT_TARGET."
        )

    def get_connection(self, catalog: str, *, target: Optional[str] = None, **kwargs):
        # Import here to avoid heavy imports at module import time and prevent cycles
        from scripts.data_generators.connections import IcebergConnection

        registry_key = self.resolve_target_for_catalog(catalog, target)
        connection_class = IcebergConnection.get_class(registry_key)
        return connection_class(**kwargs) if kwargs else connection_class()

    def close_connection(self, con) -> None:
        # Try to gracefully stop Spark if present
        try:
            spark = getattr(con, "con", None)
            if spark is not None and hasattr(spark, "stop"):
                spark.stop()
        except Exception:
            pass

    def setup(self, con) -> None:
        pass

    def generate(self, catalog: str, *, target: Optional[str] = None, connection_kwargs: Optional[dict] = None):
        con = self.get_connection(catalog, target=target, **(connection_kwargs or {}))
        try:
            self.setup(con)

            intermediate_dir = os.path.join(INTERMEDIATE_DIR, con.name, self.table)
            last_file = None
            for path in self.files:
                full_file_path = os.path.join(SCRIPT_DIR, self.table, path)
                with open(full_file_path, 'r') as file:
                    snapshot_name = os.path.basename(path)[:-4]
                    last_file = snapshot_name
                    queries = [x for x in file.read().split(';') if x.strip() != '']

                    for query in queries:
                        # Execute query on the underlying engine (e.g., Spark)
                        con.con.sql(query)

                        if self.write_intermediates:
                            namespace = '.'.join(self.namespace)
                            df = con.con.read.table(f"{namespace}.{self.table}")
                            intermediate_data_path = os.path.join(intermediate_dir, snapshot_name, 'data.parquet')
                            df.write.mode("overwrite").parquet(intermediate_data_path)

            if self.write_intermediates and last_file:
                # Finally, copy the latest results to a "last" dir for easy test writing
                shutil.copytree(
                    os.path.join(intermediate_dir, last_file, 'data.parquet'),
                    os.path.join(intermediate_dir, 'last', 'data.parquet'),
                    dirs_exist_ok=True,
                )
        finally:
            self.close_connection(con)
