# This file is included by DuckDB's build system. It specifies which extension to load
if (NOT EMSCRIPTEN)
  duckdb_extension_load(avro
  LOAD_TESTS
  GIT_URL https://github.com/duckdb/duckdb-avro
  GIT_TAG a73b60d629a92146cfd71c210936144a971783bd
)
endif()

# Extension from this repo
if (DONT_LINK OR "$ENV{DONT_LINK}")
    set(ICEBERG_DONT_LINK "DONT_LINK")
else()
    set(ICEBERG_DONT_LINK "")
endif()

duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    ${ICEBERG_DONT_LINK}
)

if (NOT EMSCRIPTEN)
  duckdb_extension_load(tpch)
  duckdb_extension_load(icu)
  duckdb_extension_load(ducklake
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/ducklake
        GIT_TAG a92abf755a7b4e2f3e410f8b89c72b990a0698da
)

  if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 18803d5e55b9f9f6dda5047d0fdb4f4238b6801d
    )
  endif()
endif()
