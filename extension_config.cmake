# This file is included by DuckDB's build system. It specifies which extension to load
if (NOT EMSCRIPTEN)
duckdb_extension_load(avro
		LOAD_TESTS
		GIT_URL https://github.com/duckdb/duckdb_avro
		GIT_TAG 4fa0f73f816e9878d5b6a39795e060877766ac1a
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
        GIT_TAG 73adbb015e31607f11c30484dd34dfc541758bdd
)

if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 18803d5e55b9f9f6dda5047d0fdb4f4238b6801d
    )
endif()
endif()
