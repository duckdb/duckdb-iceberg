# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    LINKED_LIBS "../../vcpkg_installed/wasm32-emscripten/lib/*.a"
)

duckdb_extension_load(tpch)
duckdb_extension_load(icu)
duckdb_extension_load(ducklake
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/ducklake
        GIT_TAG 9cc2d903c51d360ff3fc6afb10cf38f8eac2e25b
)

duckdb_extension_load(avro
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-avro
        GIT_TAG 180e41e8ad13b8712d207785a6bca0aa39341040
)

if (NOT EMSCRIPTEN)
################## AWS
if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG main
    )
endif ()
endif()

duckdb_extension_load(httpfs
        GIT_URL https://github.com/danklynn/duckdb-httpfs
        GIT_TAG d5a7a2b06d4b6dc5e98cd19748d1dbe3cd9c5aa6
)

