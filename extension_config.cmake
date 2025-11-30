# This file is included by DuckDB's build system. It specifies which extension to load

# Load only avro (required dependency for iceberg)
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
