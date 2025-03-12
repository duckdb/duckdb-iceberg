# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

duckdb_extension_load(avro
    LOAD_TESTS
    GIT_URL https://github.com/tishj/duckdb_avro
    GIT_TAG 0287790955f98bd8b6fd6aa2993335b16a211791
)

duckdb_extension_load(tpch)

################## AWS
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG main
    )
endif ()
