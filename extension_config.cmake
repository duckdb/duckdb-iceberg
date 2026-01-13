# This file is included by DuckDB's build system. It specifies which extension to load
if (NOT EMSCRIPTEN)
duckdb_extension_load(avro
		LOAD_TESTS
		GIT_URL https://github.com/duckdb/duckdb-avro
		GIT_TAG 356da2593bcaec2fa01dea7f450c288d68807334
)
endif()

# Extension from this repo
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

if (NOT EMSCRIPTEN)
duckdb_extension_load(tpch)
duckdb_extension_load(icu)
duckdb_extension_load(ducklake
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/ducklake
        GIT_TAG 64f3b12fa3f364efa9696ff70922d7f22325873a
)

if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 18803d5e55b9f9f6dda5047d0fdb4f4238b6801d
    )
endif()
endif()
