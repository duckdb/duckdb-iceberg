# This file is included by DuckDB's build system. It specifies which extension to load
if (NOT EMSCRIPTEN)
  duckdb_extension_load(avro
  LOAD_TESTS
  GIT_URL https://github.com/duckdb/duckdb-avro
  GIT_TAG 758fb6063cbb6da49b2e1c16c4044cfcafefc52f
  SUBMODULES "third_party/avro-c"
  APPLY_PATCHES
)
  duckdb_extension_statically_link(avro)
endif()

# Extension from this repo
duckdb_extension_load(parquet)
duckdb_extension_load(json)
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)
# Static registration order is also database startup load order. Avro above and
# Parquet here must precede Iceberg, so fresh databases (including CSV result readers)
# do not try to auto-install unsigned copies from the build's extension repository.
duckdb_extension_statically_link(parquet json iceberg)

if (NOT EMSCRIPTEN)
  # Use DuckDB's pinned HTTPFS configuration without relying on Make build flags.
  include("${EXTENSION_CONFIG_BASE_DIR}/httpfs.cmake")
  duckdb_extension_load(tpch)
  duckdb_extension_load(icu)
  duckdb_extension_statically_link(tpch icu httpfs)
  #  duckdb_extension_load(ducklake
  #        LOAD_TESTS
  #        GIT_URL https://github.com/duckdb/ducklake
  #        GIT_TAG a92abf755a7b4e2f3e410f8b89c72b990a0698da
  #)

  if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG de46483edd4aab903a481db5332cbfa7970568f1 # main 2026-10-12 pin
            APPLY_PATCHES
    )
    duckdb_extension_statically_link(aws)
  endif()
endif()

# The native REST mock is a separate, test-only extension. It is deliberately not
# linked into Iceberg or the default DuckDB library; SQL tests load it explicitly.
if (BUILD_UNITTESTS AND NOT EMSCRIPTEN)
  duckdb_extension_load(iceberg_mock
      SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/test/native_catalog
      INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/test/native_catalog
  )
endif()
