cmake_minimum_required(VERSION 3.5...3.29)

project(IcebergMockExtension)

# This extension depends only on DuckDB's extension interfaces. In particular,
# none of its sources or include paths belong to the Iceberg extension.
option(ICEBERG_MOCK_BUILD_LOADABLE
       "Build the native catalog as a loadable test extension" ON)
set(ICEBERG_MOCK_BUILD_OPTIONS)
if(NOT ICEBERG_MOCK_BUILD_LOADABLE)
  list(APPEND ICEBERG_MOCK_BUILD_OPTIONS NO_LOADABLE)
endif()
build_extension_library(
  iceberg_mock ${ICEBERG_MOCK_BUILD_OPTIONS} catalog_json.cpp catalog_model.cpp
  catalog_router.cpp catalog_transport.cpp)

add_executable(unittest_iceberg_native test_native_catalog.cpp)
target_include_directories(unittest_iceberg_native
                           PRIVATE ${CMAKE_SOURCE_DIR}/third_party/catch)
set_target_properties(
  unittest_iceberg_native PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                     ${CMAKE_BINARY_DIR}/test)
# The model tests use DuckDB internals that are not exported by the Windows DLL.
# Register only the extensions these tests need; Iceberg is exercised separately
# by the SQL suite over HTTPUtil/HTTPClient.
target_link_libraries(unittest_iceberg_native duckdb_static)
duckdb_link_extensions(unittest_iceberg_native httpfs iceberg_mock)

# Exercise the scan callbacks directly, including failures that optimizer
# fallback would otherwise hide from sqllogictests.
add_executable(unittest_iceberg_serialization test_scan_serialization.cpp)
target_include_directories(
  unittest_iceberg_serialization
  PRIVATE ${CMAKE_SOURCE_DIR}/third_party/catch
          ${CMAKE_CURRENT_LIST_DIR}/../../src/include)
set_target_properties(
  unittest_iceberg_serialization PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                            ${CMAKE_BINARY_DIR}/test)
target_link_libraries(unittest_iceberg_serialization duckdb_static)
duckdb_link_extensions(
  unittest_iceberg_serialization
  avro
  parquet
  json
  iceberg
  httpfs
  iceberg_mock)

# Drive overlapping LoadTable requests in a fixed order, without timing races.
add_executable(unittest_iceberg_cache test_load_table_cache.cpp)
target_include_directories(
  unittest_iceberg_cache PRIVATE ${CMAKE_SOURCE_DIR}/third_party/catch
                                 ${CMAKE_CURRENT_LIST_DIR}/../../src/include)
set_target_properties(
  unittest_iceberg_cache PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                    ${CMAKE_BINARY_DIR}/test)
target_link_libraries(unittest_iceberg_cache duckdb_static)
duckdb_link_extensions(
  unittest_iceberg_cache
  avro
  parquet
  json
  iceberg
  httpfs
  iceberg_mock)

add_executable(unittest_iceberg_planning test_scan_planning.cpp)
target_include_directories(
  unittest_iceberg_planning PRIVATE ${CMAKE_SOURCE_DIR}/third_party/catch
                                    ${CMAKE_CURRENT_LIST_DIR}/../../src/include)
set_target_properties(
  unittest_iceberg_planning PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                       ${CMAKE_BINARY_DIR}/test)
target_link_libraries(unittest_iceberg_planning duckdb_static)
duckdb_link_extensions(
  unittest_iceberg_planning
  avro
  parquet
  json
  iceberg
  httpfs
  iceberg_mock)
