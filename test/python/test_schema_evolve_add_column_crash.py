"""
Reproducer for crash: "Attempted to access index N within vector of size N"

Bug: After schema evolution (ADD COLUMN) without new data writes,
GetSchemaVersion returns table entry from current_schema_id (more columns),
but GetScanFunction builds scan from snapshot->schema_id (fewer columns).
Planner creates column indices beyond global_columns size, causing OOB access
in CreateColumnMappingByMapper.
"""

import pytest
import duckdb
import os

pyiceberg = pytest.importorskip("pyiceberg")
from pyiceberg.catalog import load_catalog
from pyiceberg.types import LongType, DoubleType


CATALOG_URI = "http://127.0.0.1:8181"
S3_ENDPOINT = "127.0.0.1:9000"
CLIENT_ID = "admin"
CLIENT_SECRET = "password"
TABLE_NAME = "default.test_add_column_crash_py"


@pytest.fixture
def catalog():
    """Load the REST catalog — requires ICEBERG_SERVER_AVAILABLE."""
    if not os.environ.get("ICEBERG_SERVER_AVAILABLE"):
        pytest.skip("Requires ICEBERG_SERVER_AVAILABLE")
    return load_catalog(
        "default",
        type="rest",
        uri=CATALOG_URI,
    )


def _make_duckdb_conn():
    conn = duckdb.connect()
    conn.execute("LOAD iceberg; LOAD httpfs;")
    conn.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{CLIENT_ID}',
            SECRET '{CLIENT_SECRET}',
            ENDPOINT '{S3_ENDPOINT}',
            URL_STYLE 'path',
            USE_SSL 0
        );
    """)
    conn.execute(f"""
        ATTACH '' AS my_datalake (
            TYPE ICEBERG,
            CLIENT_ID '{CLIENT_ID}',
            CLIENT_SECRET '{CLIENT_SECRET}',
            ENDPOINT '{CATALOG_URI}'
        );
    """)
    return conn


def test_add_column_crash(catalog):
    """
    Steps to reproduce:
    1. Create table with 2 columns via DuckDB, insert data
    2. Evolve schema via PyIceberg (add 2 columns) — no new data written
    3. Read table via DuckDB catalog path → crashes before fix
    """
    conn = _make_duckdb_conn()

    # Cleanup
    conn.execute(f"DROP TABLE IF EXISTS my_datalake.{TABLE_NAME}")

    # Step 1: Create table and insert data via DuckDB
    conn.execute(
        f"CREATE TABLE my_datalake.{TABLE_NAME} AS SELECT range a, range::VARCHAR b FROM range(5)"
    )
    conn.close()

    # Step 2: Evolve schema via PyIceberg — adds columns without writing data
    table = catalog.load_table(TABLE_NAME)
    with table.update_schema() as update:
        update.add_column("c", LongType())
        update.add_column("d", DoubleType())

    # At this point:
    # - current_schema_id points to new 4-column schema
    # - latest snapshot still references old 2-column schema

    # Step 3: Read via DuckDB catalog — this crashes before the fix
    conn = _make_duckdb_conn()

    result = conn.execute(
        f"SELECT * FROM my_datalake.{TABLE_NAME} ORDER BY a"
    ).fetchall()

    # Expected: 5 rows, c and d should be NULL
    assert len(result) == 5
    for i, row in enumerate(result):
        assert row[0] == i      # a
        assert row[1] == str(i) # b
        assert row[2] is None   # c (new column, no data)
        assert row[3] is None   # d (new column, no data)

    # Cleanup
    conn.execute(f"DROP TABLE IF EXISTS my_datalake.{TABLE_NAME}")
    conn.close()
