cmake_minimum_required(VERSION 3.5...3.29)

project(IcebergTestTableLoaderExtension)

# Test-only, linked into the test binary: the iceberg extension's loader
# interface is header-only
include_directories(${CMAKE_CURRENT_SOURCE_DIR}
                    ${CMAKE_CURRENT_SOURCE_DIR}/../../../src/include)

add_library(iceberg_test_table_loader OBJECT
            iceberg_test_table_loader_extension.cpp)
# DuckDB adds test/, and with it unittest, after the extensions
cmake_language(
  DEFER
  DIRECTORY
  ${DuckDB_SOURCE_DIR}
  CALL
  target_link_libraries
  unittest
  iceberg_test_table_loader)
