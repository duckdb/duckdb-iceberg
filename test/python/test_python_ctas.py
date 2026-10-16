import ctypes
import os
import pathlib
import re
import subprocess
import sys
import tempfile


# SQL PREPARE does not accept CTAS, but ADBC prepares and executes CTAS through
# DuckDB's C API. Exercise that path directly to cover issue #595 without dbt.
def _duckdb_library_path(build_dir):
    candidates = [path for path in (build_dir / "src").glob("libduckdb.*") if path.suffix in (".so", ".dylib", ".dll")]
    if not candidates:
        raise FileNotFoundError(f"libduckdb was not built in {build_dir}")
    return candidates[0]


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
    lib.duckdb_create_config.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
    lib.duckdb_create_config.restype = ctypes.c_int
    lib.duckdb_set_config.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
    lib.duckdb_set_config.restype = ctypes.c_int
    lib.duckdb_destroy_config.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
    lib.duckdb_destroy_config.restype = None
    lib.duckdb_open_ext.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_char_p),
    ]
    lib.duckdb_open_ext.restype = ctypes.c_int
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
    def __init__(self, build_dir):
        # EXTENSION_STATIC_BUILD=0 extensions resolve DuckDB symbols from the host.
        self.lib = ctypes.CDLL(str(_duckdb_library_path(build_dir)), mode=ctypes.RTLD_GLOBAL)
        _init_api(self.lib)
        self.db = ctypes.c_void_p()
        self.con = ctypes.c_void_p()
        config = ctypes.c_void_p()
        error = ctypes.c_char_p()
        try:
            assert self.lib.duckdb_create_config(ctypes.byref(config)) == 0
            assert self.lib.duckdb_set_config(config, b"allow_unsigned_extensions", b"true") == 0
            state = self.lib.duckdb_open_ext(None, ctypes.byref(self.db), config, ctypes.byref(error))
            assert state == 0, error.value.decode() if error.value else "Could not open DuckDB"
            assert self.lib.duckdb_connect(self.db, ctypes.byref(self.con)) == 0
        except BaseException:
            self.close()
            raise
        finally:
            self.lib.duckdb_destroy_config(ctypes.byref(config))
            if error:
                self.lib.duckdb_free(error)

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


def _run_ctas_test(test, build_dir, catalog_init_sql):
    db = DuckDB(build_dir)
    try:
        with tempfile.TemporaryDirectory(prefix="iceberg-capi-extensions-") as extension_dir:
            escaped_dir = extension_dir.replace("'", "''")
            db.query(f"SET extension_directory='{escaped_dir}'")
            for extension in ("core_functions", "parquet"):
                db.query(f"LOAD {extension}")
            # The local repository is also included in CI's trimmed build artifacts.
            repository = str(build_dir / "repository").replace("'", "''")
            for extension in ("avro", "httpfs", "iceberg"):
                db.query(f"INSTALL {extension} FROM '{repository}'")
                db.query(f"LOAD {extension}")
            if catalog_init_sql.strip():
                db.query(catalog_init_sql)
            test(db)
    finally:
        db.close()


def _prepared_statement_rebinds_at_execute(duckdb_capi):
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


def _duplicate_ctas_in_transaction_still_errors(duckdb_capi):
    duckdb_capi.query("DROP TABLE IF EXISTS my_datalake.default.ctas_duplicate_guard_595")
    duckdb_capi.query("BEGIN")
    duckdb_capi.query("CREATE TABLE my_datalake.default.ctas_duplicate_guard_595 AS SELECT 1 AS id")
    state, _ = duckdb_capi.query(
        "CREATE TABLE my_datalake.default.ctas_duplicate_guard_595 AS SELECT 2 AS id", expect_ok=False
    )
    assert state != 0
    duckdb_capi.query("ROLLBACK")


def _darwin_asan_runtime(library_path):
    load_commands = subprocess.check_output(["otool", "-l", str(library_path)], text=True)
    dependencies = re.findall(r"^\s+name (.+) \(offset \d+\)$", load_commands, re.MULTILINE)
    runtime = next((name for name in dependencies if pathlib.Path(name).name.startswith("libclang_rt.asan")), None)
    if runtime is None:
        return None

    def resolve_loader_path(path):
        return pathlib.Path(
            path.replace("@loader_path", str(library_path.resolve().parent)).replace(
                "@executable_path", str(pathlib.Path(sys.executable).resolve().parent)
            )
        )

    if runtime.startswith("@rpath/"):
        rpaths = re.findall(r"^\s+path (.+) \(offset \d+\)$", load_commands, re.MULTILINE)
        candidates = [resolve_loader_path(path) / runtime.removeprefix("@rpath/") for path in rpaths]
    else:
        candidates = [resolve_loader_path(runtime)]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"Could not find ASan runtime {runtime} linked by {library_path}; checked {candidates}")


def _capi_subprocess_env(build_dir):
    env = os.environ.copy()
    runtimes = []
    if sys.platform == "linux":
        # CI bundles the runtime from the compiler that instrumented libduckdb.
        runtimes = list((build_dir / "repository" / "capi_runtime").glob("libclang_rt.asan*.so"))
        preload_variable = "LD_PRELOAD"
    elif sys.platform == "darwin":
        # Resolve the runtime recorded in the library, which may differ from the current compiler.
        runtime = _darwin_asan_runtime(_duckdb_library_path(build_dir))
        if runtime:
            runtimes.append(runtime)
        preload_variable = "DYLD_INSERT_LIBRARIES"
    if runtimes:
        assert len(runtimes) == 1, f"Expected one ASan runtime, found: {runtimes}"
        env[preload_variable] = os.pathsep.join(filter(None, (str(runtimes[0]), env.get(preload_variable))))
        # CPython is not built with ASan and leaves interpreter allocations at exit.
        # Keep ASan/UBSan checks; the native unittest process still runs LSan.
        env["ASAN_OPTIONS"] = ":".join(filter(None, (env.get("ASAN_OPTIONS"), "detect_leaks=0")))
    return env


def _run_in_subprocess(test_name, build_dir, catalog_init_sql):
    _duckdb_library_path(build_dir)
    # Catalog test collection imports the installed Python duckdb package.
    # Keep its native library out of the process exercising this build's C API.
    result = subprocess.run(
        [sys.executable, str(pathlib.Path(__file__).resolve()), test_name, str(build_dir)],
        input=catalog_init_sql,
        text=True,
        capture_output=True,
        env=_capi_subprocess_env(build_dir),
    )
    assert result.returncode == 0, f"C API test failed:\n{result.stdout}\n{result.stderr}"


def test_iceberg_ctas_prepared_statement_rebinds_at_execute(run_ctas_test):
    run_ctas_test("prepared_statement_rebinds_at_execute")


def test_iceberg_duplicate_ctas_in_transaction_still_errors(run_ctas_test):
    run_ctas_test("duplicate_ctas_in_transaction_still_errors")


if __name__ == "__main__":
    tests = {
        "load_extensions": lambda db: None,
        "prepared_statement_rebinds_at_execute": _prepared_statement_rebinds_at_execute,
        "duplicate_ctas_in_transaction_still_errors": _duplicate_ctas_in_transaction_still_errors,
    }
    if sys.argv[1] == "check_build":
        _run_in_subprocess("load_extensions", pathlib.Path(sys.argv[2]).resolve(), "")
    else:
        _run_ctas_test(tests[sys.argv[1]], pathlib.Path(sys.argv[2]), sys.stdin.read())
else:
    # The standalone build check and C API worker require only the standard library.
    import pytest

    @pytest.fixture()
    def run_ctas_test(duckdb_catalog_init_sql, unittest_binary):
        build_dir = pathlib.Path(unittest_binary).resolve().parents[1]
        try:
            _duckdb_library_path(build_dir)
        except FileNotFoundError as error:
            pytest.skip(str(error))
        return lambda test_name: _run_in_subprocess(test_name, build_dir, duckdb_catalog_init_sql)
