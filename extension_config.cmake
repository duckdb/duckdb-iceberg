# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    LINKED_LIBS "../../vcpkg_installed/wasm32-emscripten/lib/*.a"
)


if (NOT ${EMSCRIPTEN})
duckdb_extension_load(tpch)
duckdb_extension_load(icu)
duckdb_extension_load(avro
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb_avro
        GIT_TAG b5cc586b440bd14a7acdeeecfa158d5b0e16f423
)
endif()

################## AWS
if (NOT MINGW AND NOT ${EMSCRIPTEN})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG main
    )
endif ()

if (NOT ${EMSCRIPTEN})
duckdb_extension_load(httpfs
        GIT_URL https://github.com/duckdb/duckdb-httpfs
        GIT_TAG eb1b04907c419d576f5fa4b34303810e8802e2f8
        INCLUDE_DIR extension/httpfs/include
)
endif ()
