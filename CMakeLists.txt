cmake_minimum_required(VERSION 3.5...3.29)

# Set extension name here
set(TARGET_NAME iceberg)
project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
include_directories(src/include)

set(EXTENSION_SOURCES
    src/iceberg_extension.cpp
    src/iceberg_functions.cpp
    src/iceberg_manifest.cpp
    src/manifest_reader.cpp
    src/catalog_api.cpp
    src/catalog_utils.cpp
    src/curl.cpp
    src/aws.cpp
    src/common/utils.cpp
    src/common/url_utils.cpp
    src/common/schema.cpp
    src/common/iceberg.cpp
    src/common/api_utils.cpp
    src/iceberg_functions/iceberg_multi_file_reader.cpp
    src/iceberg_functions/iceberg_snapshots.cpp
    src/iceberg_functions/iceberg_scan.cpp
    src/iceberg_functions/iceberg_metadata.cpp
    src/storage/authorization/sigv4.cpp
    src/storage/authorization/oauth2.cpp
    src/storage/irc_authorization.cpp
    src/storage/irc_catalog.cpp
    src/storage/irc_catalog_set.cpp
    src/storage/irc_clear_cache.cpp
    src/storage/irc_schema_entry.cpp
    src/storage/irc_schema_set.cpp
    src/storage/irc_table_entry.cpp
    src/storage/irc_table_set.cpp
    src/storage/irc_transaction.cpp
    src/storage/irc_authorization.cpp
    src/storage/irc_transaction_manager.cpp)

add_subdirectory(src/rest_catalog/objects)

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES} ${ALL_OBJECT_FILES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

find_package(CURL REQUIRED)
find_package(AWSSDK REQUIRED COMPONENTS core sso sts)
include_directories(${CURL_INCLUDE_DIRS})

# Reset the TARGET_NAME, the AWS find_package build could bleed into our build -
# overriding `TARGET_NAME`
set(TARGET_NAME iceberg)

# AWS SDK FROM vcpkg
target_include_directories(${EXTENSION_NAME}
                           PUBLIC $<BUILD_INTERFACE:${AWSSDK_INCLUDE_DIRS}>)
target_link_libraries(${EXTENSION_NAME} PUBLIC ${AWSSDK_LINK_LIBRARIES})
target_include_directories(${TARGET_NAME}_loadable_extension
                           PRIVATE $<BUILD_INTERFACE:${AWSSDK_INCLUDE_DIRS}>)
target_link_libraries(${TARGET_NAME}_loadable_extension
                      ${AWSSDK_LINK_LIBRARIES})

# Link dependencies into extension
target_link_libraries(${EXTENSION_NAME} PUBLIC ${CURL_LIBRARIES})
target_link_libraries(${TARGET_NAME}_loadable_extension ${CURL_LIBRARIES})

install(
  TARGETS ${EXTENSION_NAME} ${TARGET_NAME}_loadable_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
