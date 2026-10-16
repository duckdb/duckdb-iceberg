cmake_minimum_required(VERSION 3.5...3.29)

project(IcebergMockExtension)

# This extension depends only on DuckDB's extension interfaces. In particular,
# none of its sources or include paths belong to the Iceberg extension.
build_extension_library(iceberg_mock catalog_json.cpp catalog_model.cpp
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
