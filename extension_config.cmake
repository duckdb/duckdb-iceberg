# This file is included by DuckDB's build system. It specifies which extension to load

duckdb_extension_load(icu)
duckdb_extension_load(avro
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb_avro
        GIT_TAG 1b53c8af9973b0267406ca5a24d7e0b52f22cec3
)

# Extension from this repo
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
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

duckdb_extension_load(httpfs
        GIT_URL https://github.com/duckdb/duckdb-httpfs
        GIT_TAG 7b09112ad257249130375c0841d962eecb85662e
        INCLUDE_DIR extension/httpfs/include
)
