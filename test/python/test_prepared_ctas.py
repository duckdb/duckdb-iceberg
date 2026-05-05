import ctypes
import glob
import os
import pathlib

import pytest


# SQL PREPARE does not accept CTAS, but ADBC prepares and executes CTAS through
# DuckDB's C API. Exercise that path directly to cover issue #595 without dbt.
pytestmark = pytest.mark.skipif(
    os.getenv("ICEBERG_SERVER_AVAILABLE") is None,
    reason="Iceberg test catalog is not available",
)


def _load_duckdb_library():
    repo = pathlib.Path(__file__).resolve().parents[2]
    candidates = []
    for build_type in ("release", "debug"):
        candidates.extend(glob.glob(str(repo / "build" / build_type / "src" / "libduckdb.*")))
    candidates = [path for path in candidates if pathlib.Path(path).suffix in (".so", ".dylib", ".dll")]
    if not candidates:
        pytest.skip("libduckdb was not built")
    return ctypes.CDLL(candidates[0])


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
    lib.duckdb_prepare.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
    lib.duckdb_prepare.restype = ctypes.c_int
    lib.duckdb_prepare_error.argtypes = [ctypes.c_void_p]
    lib.duckdb_prepare_error.restype = ctypes.c_char_p
    lib.duckdb_execute_prepared_streaming.argtypes = [ctypes.c_void_p, ctypes.POINTER(DuckDBResult)]
    lib.duckdb_execute_prepared_streaming.restype = ctypes.c_int
    lib.duckdb_destroy_prepare.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
    lib.duckdb_value_int32.argtypes = [ctypes.POINTER(DuckDBResult), ctypes.c_uint64, ctypes.c_uint64]
    lib.duckdb_value_int32.restype = ctypes.c_int32
    lib.duckdb_value_varchar.argtypes = [ctypes.POINTER(DuckDBResult), ctypes.c_uint64, ctypes.c_uint64]
    lib.duckdb_value_varchar.restype = ctypes.c_void_p
    lib.duckdb_free.argtypes = [ctypes.c_void_p]


class DuckDB:
    def __init__(self):
        self.lib = _load_duckdb_library()
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

    def prepare(self, sql):
        prepared = ctypes.c_void_p()
        state = self.lib.duckdb_prepare(self.con, sql.encode(), ctypes.byref(prepared))
        error = self.lib.duckdb_prepare_error(prepared)
        assert state == 0, error.decode() if error else None
        return prepared

    def execute_prepared_streaming(self, prepared):
        result = DuckDBResult()
        state = self.lib.duckdb_execute_prepared_streaming(prepared, ctypes.byref(result))
        error = self._result_error(result)
        self.lib.duckdb_destroy_result(ctypes.byref(result))
        assert state == 0, error

    def destroy_prepare(self, prepared):
        self.lib.duckdb_destroy_prepare(ctypes.byref(prepared))

    def fetch_single_row(self, sql):
        result = DuckDBResult()
        state = self.lib.duckdb_query(self.con, sql.encode(), ctypes.byref(result))
        error = self._result_error(result)
        assert state == 0, error
        text_ptr = self.lib.duckdb_value_varchar(ctypes.byref(result), 1, 0)
        assert text_ptr
        try:
            row = (self.lib.duckdb_value_int32(ctypes.byref(result), 0, 0), ctypes.string_at(text_ptr).decode())
        finally:
            self.lib.duckdb_free(text_ptr)
            self.lib.duckdb_destroy_result(ctypes.byref(result))
        return row

    def _result_error(self, result):
        error = self.lib.duckdb_result_error(ctypes.byref(result))
        return error.decode() if error else None


@pytest.fixture()
def duckdb_capi():
    db = DuckDB()
    try:
        for extension in ("core_functions", "parquet", "avro", "httpfs", "iceberg"):
            db.query(f"LOAD {extension}")
        db.query(
            "CREATE SECRET (TYPE S3,KEY_ID 'admin',SECRET 'password',ENDPOINT '127.0.0.1:9000',"
            "URL_STYLE 'path',USE_SSL 0); "
            "ATTACH '' AS my_datalake (TYPE ICEBERG,CLIENT_ID 'admin',CLIENT_SECRET 'password',"
            "ENDPOINT 'http://127.0.0.1:8181'); "
            "CREATE SCHEMA IF NOT EXISTS my_datalake.default;"
        )
        yield db
    finally:
        db.close()


def test_iceberg_ctas_prepared_statement_rebinds_at_execute(duckdb_capi):
    duckdb_capi.query("DROP TABLE IF EXISTS my_datalake.default.ctas_prepared_rebind_595")
    prepared = duckdb_capi.prepare(
        "CREATE TABLE my_datalake.default.ctas_prepared_rebind_595 AS SELECT 42 AS id, 'prepared' AS note"
    )
    try:
        duckdb_capi.execute_prepared_streaming(prepared)
    finally:
        duckdb_capi.destroy_prepare(prepared)

    assert duckdb_capi.fetch_single_row("SELECT id, note FROM my_datalake.default.ctas_prepared_rebind_595") == (
        42,
        "prepared",
    )
    duckdb_capi.query("DROP TABLE my_datalake.default.ctas_prepared_rebind_595")


def test_iceberg_duplicate_ctas_in_transaction_still_errors(duckdb_capi):
    duckdb_capi.query("DROP TABLE IF EXISTS my_datalake.default.ctas_duplicate_guard_595")
    duckdb_capi.query("BEGIN")
    duckdb_capi.query("CREATE TABLE my_datalake.default.ctas_duplicate_guard_595 AS SELECT 1 AS id")
    state, _ = duckdb_capi.query(
        "CREATE TABLE my_datalake.default.ctas_duplicate_guard_595 AS SELECT 2 AS id", expect_ok=False
    )
    assert state != 0
    duckdb_capi.query("ROLLBACK")
