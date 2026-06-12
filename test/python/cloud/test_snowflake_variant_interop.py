"""
Cloud interop tests for VARIANT statistics between DuckDB and Snowflake.

This branch teaches DuckDB to write VARIANT lower/upper bounds into Iceberg
manifest files. Snowflake is (currently) the other engine that reads those
variant stats and uses them for file pruning. These tests exercise interop
against a Snowflake-hosted Polaris (Open Catalog) REST catalog that both
engines point at.

Topology / what makes this work
-------------------------------
DuckDB attaches the Polaris catalog directly and can WRITE to it (the backing
external volume is created with ALLOW_WRITES = TRUE). Snowflake reaches the
same catalog through a read-only POLARIS catalog integration, so on the
Snowflake side a table created by DuckDB must be *registered* as an external
Iceberg table before it can be read:

    CREATE OR REPLACE ICEBERG TABLE <name>
      EXTERNAL_VOLUME = 'iceberg_external_volume'
      CATALOG = 's3_catalog_integration'
      CATALOG_NAMESPACE = 'default'
      CATALOG_TABLE_NAME = '<name>';

Because that registration is read-only in Snowflake, the proven direction is
DuckDB-writes / Snowflake-reads (test_snowflake_reads_duckdb_written_variant).
The reverse (Snowflake writes a table DuckDB reads) needs Snowflake to have
write access to the shared catalog, which the read-only integration does not
grant, so that test is skipped and kept as documentation of the intended flow.

Required Snowflake setup (run once in a worksheet; see scripts/ for the SQL):
  - EXTERNAL VOLUME 'iceberg_external_volume' (ALLOW_WRITES = TRUE)
  - CATALOG INTEGRATION 's3_catalog_integration' (CATALOG_SOURCE = POLARIS)

The DuckDB side is driven through the C API (libduckdb) loaded with ctypes so
the test always runs against the *current* build of the extension, mirroring
test/python/test_python_ctas.py. The Snowflake side is driven through the
snowflake-snowpark-python package (`Session.sql(...)`).

Required environment variables
------------------------------
  SNOWFLAKE_PAT                   Programmatic Access Token for the Snowflake
                                  user (account/user/role/warehouse/database
                                  are hard-coded in the snowflake_session
                                  fixture; the Polaris OAuth client is
                                  hard-coded in the duckdb_con fixture).

Optional overrides (defaults below match the scripts/ setup):
  ICEBERG_CATALOG_ENDPOINT, ICEBERG_CATALOG_NAME, ICEBERG_CATALOG_REGION,
  SNOWFLAKE_EXTERNAL_VOLUME, SNOWFLAKE_CATALOG_INTEGRATION
"""

import ctypes
import glob
import os
import pathlib

import pytest


# ---------------------------------------------------------------------------
# Load libduckdb BEFORE importing snowpark.
#
# snowpark pulls in pyarrow, and pyarrow + libduckdb carry overlapping bundled
# native symbols (Arrow). Whichever is loaded into the process first wins symbol
# resolution; if pyarrow loads first, some of libduckdb's symbols resolve to
# pyarrow's and duckdb_open() segfaults. Loading libduckdb first pins its own
# symbols. Loading it first is sufficient -- the open() call may happen later.
# ---------------------------------------------------------------------------


def _find_duckdb_library():
    components = pathlib.Path(__file__).resolve()
    parent_id = 0
    repo = None
    while len(components.parents) > parent_id:
        if components.parents[parent_id].parts[-1] == "duckdb-iceberg":
            repo = components.parents[parent_id]
            break
        parent_id += 1
    pytest.mark.skipif(repo is None, "Could not find duckdb-iceberg extension build")
    candidates = []
    for build_type in ["release"]:
        candidates.extend(glob.glob(str(repo / "build" / build_type / "src" / "libduckdb.*")))
    candidates = [path for path in candidates if pathlib.Path(path).suffix in (".so", ".dylib", ".dll")]
    return candidates[0] if candidates else None


_DUCKDB_LIB_PATH = _find_duckdb_library()
_DUCKDB_LIB = ctypes.CDLL(_DUCKDB_LIB_PATH) if _DUCKDB_LIB_PATH else None

# Now that libduckdb is pinned, it is safe to import snowpark / pyarrow.
snowpark = pytest.importorskip("snowflake.snowpark")
from snowflake.snowpark import Session  # noqa: E402

# The shared Iceberg namespace both engines operate in (DuckDB writes here;
# Snowflake references it via CATALOG_NAMESPACE when registering).
NAMESPACE = "default"

CATALOG_NAME = os.environ.get("ICEBERG_CATALOG_NAME", "s3-catalog")

# Snowflake objects used to register an externally-created Iceberg table.
EXTERNAL_VOLUME = os.environ.get("SNOWFLAKE_EXTERNAL_VOLUME", "iceberg_external_volume")
CATALOG_INTEGRATION = os.environ.get("SNOWFLAKE_CATALOG_INTEGRATION", "s3_catalog_integration")

REQUIRED_ENV_VARS = [
    "ICEBERG_SNOWFLAKE_REMOTE_AVAILABLE",
    "SNOWFLAKE_PAT",
    "ICEBERG_CATALOG_CLIENT_ID",
    "ICEBERG_CATALOG_ENDPOINT",
    "ICEBERG_CATALOG_CLIENT_SECRET",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
]


_missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]

pytestmark = pytest.mark.skipif(
    bool(_missing),
    reason=f"missing env vars for snowflake geometry interop: {', '.join(_missing)}",
)

# ---------------------------------------------------------------------------
# Minimal DuckDB C-API wrapper (ctypes) so we run against the current build.
# Mirrors the binding approach used in test/python/test_python_ctas.py.
# ---------------------------------------------------------------------------


class DuckDBResult(ctypes.Structure):
    _fields_ = [
        ("deprecated_column_count", ctypes.c_uint64),
        ("deprecated_row_count", ctypes.c_uint64),
        ("deprecated_rows_changed", ctypes.c_uint64),
        ("deprecated_columns", ctypes.c_void_p),
        ("deprecated_error_message", ctypes.c_char_p),
        ("internal_data", ctypes.c_void_p),
    ]


def _init_api(lib):
    lib.duckdb_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
    lib.duckdb_open.restype = ctypes.c_int
    lib.duckdb_connect.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
    lib.duckdb_connect.restype = ctypes.c_int
    lib.duckdb_disconnect.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
    lib.duckdb_close.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
    lib.duckdb_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(DuckDBResult)]
    lib.duckdb_query.restype = ctypes.c_int
    lib.duckdb_result_error.argtypes = [ctypes.POINTER(DuckDBResult)]
    lib.duckdb_result_error.restype = ctypes.c_char_p
    lib.duckdb_destroy_result.argtypes = [ctypes.POINTER(DuckDBResult)]
    lib.duckdb_column_count.argtypes = [ctypes.POINTER(DuckDBResult)]
    lib.duckdb_column_count.restype = ctypes.c_uint64
    lib.duckdb_row_count.argtypes = [ctypes.POINTER(DuckDBResult)]
    lib.duckdb_row_count.restype = ctypes.c_uint64
    lib.duckdb_value_varchar.argtypes = [ctypes.POINTER(DuckDBResult), ctypes.c_uint64, ctypes.c_uint64]
    lib.duckdb_value_varchar.restype = ctypes.c_void_p
    lib.duckdb_free.argtypes = [ctypes.c_void_p]


class DuckDB:
    def __init__(self):
        if _DUCKDB_LIB is None:
            pytest.skip("libduckdb was not built")
        self.lib = _DUCKDB_LIB
        _init_api(self.lib)
        self.db = ctypes.c_void_p()
        self.con = ctypes.c_void_p()
        assert self.lib.duckdb_open(None, ctypes.byref(self.db)) == 0
        assert self.lib.duckdb_connect(self.db, ctypes.byref(self.con)) == 0

    def close(self):
        if self.con:
            self.lib.duckdb_disconnect(ctypes.byref(self.con))
        if self.db:
            self.lib.duckdb_close(ctypes.byref(self.db))

    def query(self, sql, expect_ok=True):
        result = DuckDBResult()
        state = self.lib.duckdb_query(self.con, sql.encode(), ctypes.byref(result))
        error = self._result_error(result)
        self.lib.duckdb_destroy_result(ctypes.byref(result))
        if expect_ok:
            assert state == 0, error
        return state, error

    def fetch_all(self, sql):
        """Run a query and return all rows as tuples of strings (NULL -> None)."""
        result = DuckDBResult()
        state = self.lib.duckdb_query(self.con, sql.encode(), ctypes.byref(result))
        error = self._result_error(result)
        assert state == 0, error
        try:
            ncol = self.lib.duckdb_column_count(ctypes.byref(result))
            nrow = self.lib.duckdb_row_count(ctypes.byref(result))
            rows = []
            for r in range(nrow):
                row = []
                for c in range(ncol):
                    ptr = self.lib.duckdb_value_varchar(ctypes.byref(result), c, r)
                    if ptr:
                        try:
                            row.append(ctypes.string_at(ptr).decode())
                        finally:
                            self.lib.duckdb_free(ptr)
                    else:
                        row.append(None)
                rows.append(tuple(row))
            return rows
        finally:
            self.lib.duckdb_destroy_result(ctypes.byref(result))

    def fetch_scalar(self, sql):
        rows = self.fetch_all(sql)
        assert rows, f"query returned no rows: {sql}"
        return rows[0][0]

    def _result_error(self, result):
        error = self.lib.duckdb_result_error(ctypes.byref(result))
        return error.decode() if error else None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def duckdb_con():
    """A DuckDB connection attached to the shared Iceberg REST catalog."""
    db = DuckDB()
    try:
        for extension in ("core_functions", "parquet", "avro", "httpfs", "icu", "iceberg"):
            db.query(f"LOAD {extension}")

        db.query(
            "CREATE SECRET iceberg_interop_secret ("
            "  TYPE ICEBERG,"
            f"  CLIENT_ID '{os.getenv('ICEBERG_CATALOG_CLIENT_ID')}',"
            f"  CLIENT_SECRET '{os.getenv('ICEBERG_CATALOG_CLIENT_SECRET')}',"
            f"  ENDPOINT '{os.getenv('ICEBERG_CATALOG_ENDPOINT')}'"
            ");"
        )

        region = os.getenv("ICEBERG_CATALOG_REGION")
        region_clause = f"  default_region '{region}'," if region else ""
        db.query(
            f"ATTACH '{CATALOG_NAME}' AS my_datalake ("
            "  TYPE ICEBERG,"
            f"{region_clause}"
            f"  ENDPOINT '{os.getenv('ICEBERG_CATALOG_ENDPOINT')}'"
            ");"
        )
        db.query(f"CREATE SCHEMA IF NOT EXISTS my_datalake.{NAMESPACE};")
        yield db
    finally:
        db.close()


@pytest.fixture(scope="module")
def snowflake_session():
    """A snowpark Session authenticated with a Programmatic Access Token."""
    config = {
        "account": f"{os.getenv('SNOWFLAKE_ACCOUNT')}",
        "user": f"{os.getenv('SNOWFLAKE_USER')}",
        "authenticator": "PROGRAMMATIC_ACCESS_TOKEN",
        "token": os.environ.get("SNOWFLAKE_PAT", ""),
        "role": f"{os.getenv('SNOWFLAKE_ROLE')}",
        "warehouse": f"{os.getenv('SNOWFLAKE_WAREHOUSE')}",
        "database": f"{os.getenv('SNOWFLAKE_DATABASE')}",
        "schema": "PUBLIC",
    }
    session = Session.builder.configs(config).create()
    try:
        yield session
    finally:
        session.close()


def _register_external_table(session, catalog_table, sf_name=None):
    """
    Register a table that already exists in the Polaris catalog as an external
    Iceberg table in Snowflake so it can be read. Returns the Snowflake-side
    table name. This registration is read-only on the Snowflake side.
    """
    sf_name = sf_name or catalog_table
    session.sql(
        f"CREATE OR REPLACE ICEBERG TABLE {sf_name}\n"
        f"  EXTERNAL_VOLUME = '{EXTERNAL_VOLUME}'\n"
        f"  CATALOG = '{CATALOG_INTEGRATION}'\n"
        f"  CATALOG_NAMESPACE = '{NAMESPACE}'\n"
        f"  CATALOG_TABLE_NAME = '{catalog_table}'"
    ).collect()
    return sf_name


# ---------------------------------------------------------------------------
# Test 1: DuckDB writes VARIANT data, Snowflake registers + reads it.
# ---------------------------------------------------------------------------

D2S_TABLE = "variant_interop_duckdb_to_snowflake"


@pytest.fixture()
def duckdb_created_d2s_table(duckdb_con):
    """
    Pre-processing step: DuckDB creates the empty VARIANT table that will be
    populated by DuckDB and read by Snowflake.
    """
    duckdb_con.query(f"DROP TABLE IF EXISTS my_datalake.{NAMESPACE}.{D2S_TABLE};")
    duckdb_con.query(f"CREATE TABLE my_datalake.{NAMESPACE}.{D2S_TABLE} (v VARIANT) " "WITH ('format-version'='3');")
    yield D2S_TABLE
    duckdb_con.query(f"DROP TABLE IF EXISTS my_datalake.{NAMESPACE}.{D2S_TABLE};")


@pytest.mark.skip("Snowflake cannot read duckdb non-shredded variant. See duckdb-internal/issues/9653")
def test_snowflake_reads_duckdb_written_variant(duckdb_con, snowflake_session, duckdb_created_d2s_table):
    table = duckdb_created_d2s_table

    # data file 1: uniformly-typed object fields -> fully shredded -> bounds emitted
    duckdb_con.query(
        f"INSERT INTO my_datalake.{NAMESPACE}.{table} "
        "SELECT {'age': i, 'city': 'city_' || i}::VARIANT FROM range(100) t(i);"
    )

    # data file 2: 'age' is INT for the first half and VARCHAR for the rest ->
    # not fully shredded -> no usable bounds for that field.
    duckdb_con.query(
        f"INSERT INTO my_datalake.{NAMESPACE}.{table} "
        "SELECT CASE WHEN i < 50 THEN {'age': i}::VARIANT "
        "            ELSE {'age': 'str_' || i}::VARIANT END "
        "FROM range(100) t(i);"
    )

    # Sanity: DuckDB sees everything it wrote.
    assert duckdb_con.fetch_scalar(f"SELECT count(*) FROM my_datalake.{NAMESPACE}.{table};") == "200"

    # Snowflake must register the DuckDB-created table before it can read it.
    sf_table = _register_external_table(snowflake_session, table)
    try:
        total = snowflake_session.sql(f"SELECT count(*) FROM {sf_table}").collect()[0][0]
        assert total == 200

        # The fully-shredded 'age' values from data file 1 round-trip and remain
        # filterable on the Snowflake side (this is the path variant stats enable).
        shredded = snowflake_session.sql(
            f"SELECT count(*) FROM {sf_table} WHERE v:age::int BETWEEN 0 AND 99"
        ).collect()[0][0]
        assert shredded >= 100

        # A specific shredded row is readable with its object fields intact.
        row = snowflake_session.sql(
            f"SELECT v:age::int AS age, v:city::string AS city FROM {sf_table} " "WHERE v:city::string = 'city_42'"
        ).collect()
        assert len(row) == 1
        assert row[0]["AGE"] == 42
        assert row[0]["CITY"] == "city_42"
    finally:
        snowflake_session.sql(f"DROP ICEBERG TABLE IF EXISTS {sf_table}").collect()


# ---------------------------------------------------------------------------
# Test 2: Snowflake writes VARIANT data, DuckDB reads it.
# ---------------------------------------------------------------------------


def test_duckdb_reads_snowflake_written_variant(duckdb_con, snowflake_session, duckdb_created_d2s_table):
    # Snowflake would need to own/create the table to write it (illustrative).
    sf_table = D2S_TABLE
    snowflake_session.sql(
        f"CREATE OR REPLACE ICEBERG TABLE {sf_table} \n"
        f"  EXTERNAL_VOLUME = '{EXTERNAL_VOLUME}'\n"
        f"  CATALOG = '{CATALOG_INTEGRATION}'\n"
        f"  CATALOG_NAMESPACE = '{NAMESPACE}'\n"
        f"  CATALOG_TABLE_NAME = '{sf_table}'"
    ).collect()
    # Have snowflake insert shredded variant stats (file 1)
    snowflake_session.sql(
        f"INSERT INTO {sf_table} "
        "SELECT TO_VARIANT(OBJECT_CONSTRUCT('age', seq4(), 'city', 'city_' || seq4())) "
        "FROM TABLE(GENERATOR(ROWCOUNT => 10))"
    ).collect()
    # Have snowflake insert shredded variant stats (file 2).
    snowflake_session.sql(
        f"INSERT INTO {sf_table} "
        "SELECT TO_VARIANT(OBJECT_CONSTRUCT('age', seq4() + 500, 'city', 'city_' || (seq4() + 500))) "
        "FROM TABLE(GENERATOR(ROWCOUNT => 10))"
    ).collect()

    # Have snowflake insert unshredded variant data into the table (file 3).
    snowflake_session.sql(
        f"INSERT INTO {sf_table} "
        "SELECT IFF(seq4() < 5, "
        "  TO_VARIANT('scalar_' || seq4()), "
        "  TO_VARIANT(OBJECT_CONSTRUCT('age', 'str_' || seq4(), 'city', 'town_' || seq4()))) "
        "FROM TABLE(GENERATOR(ROWCOUNT => 10))"
    ).collect()

    # Have snowflake insert unshredded variant data into the table (file 4),
    snowflake_session.sql(
        f"INSERT INTO {sf_table} "
        "SELECT IFF(seq4() < 5, "
        "  TO_VARIANT('scalar_' || (seq4() + 1000)), "
        "  TO_VARIANT(OBJECT_CONSTRUCT('age', 'str_' || (seq4() + 1000), 'city', 'town_' || (seq4() + 1000)))) "
        "FROM TABLE(GENERATOR(ROWCOUNT => 10))"
    ).collect()

    duckdb_con.query("call enable_logging('Iceberg')")
    # DuckDB can filter on the shredded variant stats
    row = duckdb_con.fetch_all(
        f"SELECT v.age::int, v.city::varchar FROM my_datalake.{NAMESPACE}.{sf_table} "
        "WHERE v.city::varchar = 'city_2';"
    )
    assert row == [("2", "city_2")]
    logs = duckdb_con.fetch_all(
        "select count(*) from duckdb_logs() where type = 'Iceberg' and message like '%skipped%'"
    )
    # only data file 2 can be skipped, it is shredded and bounds do not match the filter
    assert logs[0][0] == '1'

    duckdb_con.query("call truncate_duckdb_logs()")
    # Have DuckDB Filter on the unshredded variant stats
    unshredded = duckdb_con.fetch_all(
        f"SELECT v.age::varchar, v.city::varchar FROM my_datalake.{NAMESPACE}.{sf_table} "
        "WHERE v.age::varchar = 'str_7';"
    )
    assert unshredded == [("str_7", "town_7")]
    logs = duckdb_con.fetch_all(
        "select count(*) from duckdb_logs() where type = 'Iceberg' and message like '%skipped%'"
    )
    # data files 1 & 2 are skipped because they start with 'town_'
    assert logs[0][0] == '2'
