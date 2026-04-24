# test/cpp/CMakeLists.txt
#
# Standalone build (from repo root): g++ -std=c++14 -O2 \ -Isrc/include
# -Iduckdb/src/include -Iduckdb/third_party/catch \ -o build/iceberg_unit_tests
# \ test/cpp/test_main.cpp test/cpp/test_sigv4_utils.cpp
# test/cpp/test_credential_scope.cpp \ -Lbuild/release/src -lduckdb
# -Wl,-rpath,build/release/src -lpthread -ldl

cmake_minimum_required(VERSION 3.5)

file(GLOB TEST_SOURCES "*.cpp")

add_executable(iceberg_unit_tests ${TEST_SOURCES})

target_include_directories(
  iceberg_unit_tests
  PRIVATE ${CMAKE_SOURCE_DIR}/src/include ${CMAKE_SOURCE_DIR}/duckdb/src/include
          ${CMAKE_SOURCE_DIR}/duckdb/third_party/catch)

target_link_libraries(iceberg_unit_tests PRIVATE duckdb)

set_target_properties(iceberg_unit_tests PROPERTIES CXX_STANDARD 14
                                                    CXX_STANDARD_REQUIRED ON)
