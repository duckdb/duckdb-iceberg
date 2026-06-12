# This file is included by DuckDB's build system. It specifies which extension to load
if (NOT EMSCRIPTEN)
  # TEMPORARY: points at the avro fork branch that implements copy_to_get_written_statistics
  # (duckdb/duckdb-avro#105). Revert GIT_URL to https://github.com/duckdb/duckdb-avro and bump
  # GIT_TAG to the merge commit once that PR lands. This extension's manifest writer uses that
  # callback to record manifest_length without a read-back HEAD request on object stores.
  duckdb_extension_load(avro
  LOAD_TESTS
  GIT_URL https://github.com/cjnoname/duckdb-avro
  GIT_TAG 5bc4505d0877aa9bf13c486661374f5134c142e1
)
endif()

# Extension from this repo
if (DONT_LINK OR "$ENV{DONT_LINK}")
  set(ICEBERG_DONT_LINK "DONT_LINK")
else()
  set(ICEBERG_DONT_LINK "")
endif()


duckdb_extension_load(json)
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
            GIT_TAG ebce8e46e9a02576cfd0296fe29c23aeeddaa937
    )
  endif()
endif()
