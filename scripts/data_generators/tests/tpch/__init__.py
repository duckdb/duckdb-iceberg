from scripts.data_generators.tests.base import IcebergTest
from scripts.data_generators.connections.base import IcebergConnection
import pathlib
import tempfile
from typing import Type, List, Optional
import duckdb


@IcebergTest.register()
class Test(IcebergTest):
    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(path.parent.name)

        # Create a temporary directory
        self.tempdir = pathlib.Path(tempfile.mkdtemp())
        self.parquet_file = self.tempdir / "tmp.parquet"

        self.duckdb_con = duckdb.connect()
        self.duckdb_con.execute("call dbgen(sf=1)")

    def generate(self, catalog: str, *, target: Optional[str] = None, connection_kwargs: Optional[dict] = None):
        con = self.get_connection(catalog, target=target, **(connection_kwargs or {}))
        self.setup(con)

    def setup(self, con):
        for tbl in ['lineitem', 'customer', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']:
            create_statement = f"""
                CREATE or REPLACE TABLE default.{tbl}
                TBLPROPERTIES (
                    'format-version'='2',
                    'write.update.mode'='merge-on-read'
                )
                AS SELECT * FROM parquet_file_view;
            """
            self.duckdb_con.execute(f"copy {tbl} to '{self.parquet_file}' (FORMAT PARQUET)")
            con.con.read.parquet(self.parquet_file.as_posix()).createOrReplaceTempView('parquet_file_view')
            con.con.sql(create_statement)
