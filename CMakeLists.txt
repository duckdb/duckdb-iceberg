cmake_minimum_required(VERSION 3.5...3.29)

# Set extension name here
set(TARGET_NAME iceberg)
project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 14 CACHE STRING "C++ standard")
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
include_directories(src/include)


set(EXTENSION_SOURCES
	src/catalog/rest/api/api_utils.cpp
	src/catalog/rest/api/catalog_api.cpp
	src/catalog/rest/api/catalog_utils.cpp
	src/catalog/rest/api/iceberg_add_snapshot.cpp
	src/catalog/rest/api/iceberg_create_table_request.cpp
	src/catalog/rest/api/iceberg_table_update.cpp
	src/catalog/rest/api/iceberg_type.cpp
	src/catalog/rest/api/table_update.cpp
	src/catalog/rest/api/url_utils.cpp
	src/catalog/rest/catalog_entry/iceberg_schema_entry.cpp
	src/catalog/rest/catalog_entry/table/iceberg_table_entry.cpp
	src/catalog/rest/catalog_entry/table/iceberg_table_information.cpp
	src/catalog/rest/iceberg_catalog.cpp
	src/catalog/rest/iceberg_schema_set.cpp
	src/catalog/rest/iceberg_table_set.cpp
	src/catalog/rest/storage/authorization/none.cpp
	src/catalog/rest/storage/authorization/oauth2.cpp
	src/catalog/rest/storage/authorization/sigv4.cpp
	src/catalog/rest/storage/aws.cpp
	src/catalog/rest/storage/iceberg_authorization.cpp
	src/catalog/rest/transaction/iceberg_transaction_data.cpp
	src/catalog/rest/transaction/iceberg_transaction_manager.cpp
	src/catalog/rest/transaction/iceberg_transaction.cpp
	src/common/iceberg_utils.cpp
	src/core/deletes/iceberg_deletion_vector.cpp
	src/core/deletes/iceberg_equality_delete.cpp
	src/core/deletes/iceberg_positional_delete.cpp
	src/core/expression/iceberg_hash.cpp
	src/core/expression/iceberg_transform.cpp
	src/core/expression/iceberg_value.cpp
	src/core/metadata/iceberg_table_metadata.cpp
	src/core/metadata/manifest/iceberg_manifest_list.cpp
	src/core/metadata/manifest/iceberg_manifest.cpp
	src/core/metadata/partition/iceberg_partition_spec.cpp
	src/core/metadata/schema/iceberg_column_definition.cpp
	src/core/metadata/schema/iceberg_field_mapping.cpp
	src/core/metadata/schema/iceberg_table_schema.cpp
	src/core/metadata/snapshot/iceberg_snapshot.cpp
	src/core/metadata/sort/iceberg_sort_order.cpp
	src/execution/operator/copy/iceberg_copy.cpp
	src/execution/operator/iceberg_delete.cpp
	src/execution/operator/iceberg_insert.cpp
	src/execution/operator/iceberg_update.cpp
	src/function/copy/iceberg_copy_function.cpp
	src/function/ducklake/iceberg_to_ducklake.cpp
	src/function/iceberg_functions.cpp
	src/function/metadata/iceberg_column_stats.cpp
	src/function/metadata/iceberg_metadata.cpp
	src/function/metadata/iceberg_partition_stats.cpp
	src/function/metadata/iceberg_snapshots.cpp
	src/function/metadata/iceberg_table_properties_functions.cpp
	src/function/scan/iceberg_scan.cpp
	src/iceberg_extension.cpp
	src/iceberg_logging.cpp
	src/planning/iceberg_multi_file_list.cpp
	src/planning/iceberg_multi_file_reader.cpp
	src/planning/metadata_io/avro/avro_scan.cpp
	src/planning/metadata_io/avro/iceberg_avro_multi_file_list.cpp
	src/planning/metadata_io/avro/iceberg_avro_multi_file_reader.cpp
	src/planning/metadata_io/base_manifest_reader.cpp
	src/planning/metadata_io/deletes/iceberg_deletes_file_reader.cpp
	src/planning/metadata_io/manifest/bound_iceberg_manifest_entry.cpp
	src/planning/metadata_io/manifest/iceberg_manifest_reader.cpp
	src/planning/metadata_io/manifest_list/bound_iceberg_manifest_list_entry.cpp
	src/planning/metadata_io/manifest_list/iceberg_manifest_list_reader.cpp
	src/planning/pruning/iceberg_predicate.cpp
	src/planning/snapshot/iceberg_snapshot_lookup.cpp
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
