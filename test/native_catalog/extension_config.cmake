# Shared by Iceberg's standalone build and DuckDB's extension CI configuration.
# The mock stays separate from Iceberg and is loaded explicitly by SQL tests.
if(BUILD_UNITTESTS AND NOT EMSCRIPTEN AND NOT EXTENSION_TESTS_ONLY)
  duckdb_extension_load(iceberg_mock
      SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}"
      INCLUDE_DIR "${CMAKE_CURRENT_LIST_DIR}"
  )
endif()
