# This file is included by DuckDB's build system. It specifies which extension to load
if (NOT EMSCRIPTEN)
duckdb_extension_load(avro
		LOAD_TESTS
		GIT_URL https://github.com/duckdb/duckdb-avro
		GIT_TAG 7b75062f6345d11c5342c09216a75c57342c2e82
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
        GIT_TAG f134ad86f2f6e7cdf4133086c38ecd9c48f1a772
)

if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 18803d5e55b9f9f6dda5047d0fdb4f4238b6801d
    )
endif()
endif()
