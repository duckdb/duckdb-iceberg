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
        GIT_TAG f84ab92354579d64a4f0a2a2f40335260972fae4
	APPLY_PATCHES
)

duckdb_extension_load(avro
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-avro
        GIT_TAG ff766174cc6cc9c4ed93fc4b75871bcdffcc6e65
        APPLY_PATCHES
)

if (NOT EMSCRIPTEN)
################## AWS
if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 880da03202acc973d6ee7f3a0423dae5a6dea83b
            APPLY_PATCHES
    )
endif ()
endif()

duckdb_extension_load(httpfs
        GIT_URL https://github.com/duckdb/duckdb-httpfs
        GIT_TAG cb5b2825eff68fc91f47e917ba88bf2ed84c2dd3
        INCLUDE_DIR extension/httpfs/include
        APPLY_PATCHES
)
