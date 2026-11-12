cmake_minimum_required(VERSION 3.5...3.29)

project(IcebergTestCredentialsExtension)

# An IcebergFileCredentialsCallback for the sqllogictests of file credentials
# under test/sql/local/native_catalog
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../src/include)
add_library(iceberg_test_credentials OBJECT
            iceberg_test_credentials_extension.cpp)

# DuckDB adds test/, and with it unittest, after the extensions, and decides
# which extensions it links (DUCKDB_EXTENSION_ICEBERG_SHOULD_LINK) after it has
# added their directories. So the link is deferred to the end of DuckDB's
# top-level directory, and made only when iceberg is linked into the same
# binary.
function(iceberg_test_credentials_link_unittest)
  if(DUCKDB_EXTENSION_ICEBERG_SHOULD_LINK)
    target_link_libraries(unittest iceberg_test_credentials)
  endif()
endfunction()
cmake_language(DEFER DIRECTORY ${DuckDB_SOURCE_DIR} CALL
               iceberg_test_credentials_link_unittest)
