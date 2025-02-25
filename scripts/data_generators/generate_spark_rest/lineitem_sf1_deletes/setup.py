import duckdb
import os

PARQUET_SRC_FILE = os.getenv('PARQUET_SRC_FILE')

duckdb_con = duckdb.connect()
duckdb_con.execute("call dbgen(sf=1)")
duckdb_con.execute(f"copy lineitem to '{PARQUET_SRC_FILE}' (FORMAT PARQUET)")