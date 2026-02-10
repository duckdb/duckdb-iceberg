cmake_minimum_required(VERSION 3.5...3.29)

# Set extension name here
set(TARGET_NAME iceberg)
project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 14 CACHE STRING "C++ standard")
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
include_directories(src/include)


set(EXTENSION_SOURCES
    src/iceberg_extension.cpp
    src/iceberg_functions.cpp
    src/iceberg_manifest.cpp
    src/iceberg_manifest_list.cpp
    src/avro_scan.cpp
    src/iceberg_snapshot_lookup.cpp
    src/catalog_api.cpp
    src/iceberg_logging.cpp
    src/catalog_utils.cpp
    src/storage/iceberg_insert.cpp
    src/storage/iceberg_delete.cpp
    src/storage/iceberg_update.cpp
    src/aws.cpp
    src/hash_utils.cpp
    src/base_manifest_reader.cpp
    src/manifest_list_reader.cpp
    src/manifest_reader.cpp
    src/deletes/equality_delete.cpp
    src/deletes/positional_delete.cpp
    src/deletes/deletion_vector.cpp
    src/metadata/iceberg_transform.cpp
    src/metadata/iceberg_table_schema.cpp
    src/metadata/iceberg_partition_spec.cpp
    src/metadata/iceberg_sort_order.cpp
    src/metadata/iceberg_snapshot.cpp
    src/metadata/iceberg_field_mapping.cpp
    src/metadata/iceberg_column_definition.cpp
    src/metadata/iceberg_table_metadata.cpp
    src/iceberg_predicate.cpp
    src/iceberg_value.cpp
    src/common/utils.cpp
    src/common/url_utils.cpp
    src/common/iceberg.cpp
    src/common/api_utils.cpp
    src/iceberg_functions/iceberg_multi_file_reader.cpp
    src/iceberg_functions/iceberg_avro_multi_file_reader.cpp
    src/iceberg_functions/iceberg_avro_multi_file_list.cpp
    src/iceberg_functions/iceberg_deletes_file_reader.cpp
    src/iceberg_functions/iceberg_multi_file_list.cpp
    src/iceberg_functions/iceberg_snapshots.cpp
    src/iceberg_functions/iceberg_scan.cpp
    src/iceberg_functions/iceberg_metadata.cpp
    src/iceberg_functions/iceberg_column_stats.cpp
    src/iceberg_functions/iceberg_partition_stats.cpp
    src/iceberg_functions/iceberg_table_properties_functions.cpp
    src/iceberg_functions/iceberg_to_ducklake.cpp
    src/storage/authorization/sigv4.cpp
    src/storage/authorization/none.cpp
    src/storage/authorization/oauth2.cpp
    src/storage/iceberg_transaction_data.cpp
    src/storage/iceberg_authorization.cpp
    src/storage/catalog/iceberg_catalog.cpp
    src/storage/catalog/iceberg_schema_entry.cpp
    src/storage/catalog/iceberg_schema_set.cpp
    src/storage/catalog/iceberg_table_entry.cpp
    src/storage/catalog/iceberg_table_set.cpp
    src/storage/iceberg_transaction.cpp
    src/storage/iceberg_authorization.cpp
    src/storage/iceberg_transaction_manager.cpp
    src/utils/iceberg_type.cpp
    src/storage/table_update/iceberg_add_snapshot.cpp
    src/storage/table_update/common.cpp
    src/storage/iceberg_table_information.cpp
    src/storage/create_table/iceberg_create_table_request.cpp
)

add_subdirectory(src/rest_catalog/objects)

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES} ${ALL_OBJECT_FILES})

set(PARAMETERS "-warnings")

if(EMSCRIPTEN)
  set (DUCKDB_EXTENSION_ICEBERG_LINKED_LIBS "../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-mqtt.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-kinesis.a;../../vcpkg_installed/wasm32-emscripten/lib/liblzma.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-auth.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-s3.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-s3.a;../../vcpkg_installed/wasm32-emscripten/lib/libroaring.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-cal.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-sdkutils.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-sso.a;../../vcpkg_installed/wasm32-emscripten/lib/libs2n.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-common.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-checksums.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-sts.a;../../vcpkg_installed/wasm32-emscripten/lib/libsnappy.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-compression.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-cognito-identity.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-crt-cpp.a;../../vcpkg_installed/wasm32-emscripten/lib/libssl.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-event-stream.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-core.a;../../vcpkg_installed/wasm32-emscripten/lib/libcrypto.a;../../vcpkg_installed/wasm32-emscripten/lib/libz.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-http.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-dynamodb.a;../../vcpkg_installed/wasm32-emscripten/lib/libcurl.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-io.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-identity-management.a;../../vcpkg_installed/wasm32-emscripten/lib/libjansson.a;../../third_party/mbedtls/libduckdb_mbedtls.a")
endif()


build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES} ${ALL_OBJECT_FILES})

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    find_package(CURL REQUIRED)
    find_package(AWSSDK REQUIRED COMPONENTS core sso sts)
    include_directories(${CURL_INCLUDE_DIRS})
endif()

# Roaring is installed via vcpkg and used here
find_package(roaring CONFIG REQUIRED)

# Reset the TARGET_NAME, the AWS find_package build could bleed into our build -
# overriding `TARGET_NAME`
set(TARGET_NAME iceberg)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
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
endif()

# Link Roaring library
target_link_libraries(${EXTENSION_NAME} PUBLIC roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)
target_link_libraries(${TARGET_NAME}_loadable_extension roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)

install(
  TARGETS ${EXTENSION_NAME} ${TARGET_NAME}_loadable_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
