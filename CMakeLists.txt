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
    src/manifest_file_reader.cpp
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
    src/iceberg_functions/iceberg_deletes_file_reader.cpp
    src/iceberg_functions/iceberg_multi_file_list.cpp
    src/iceberg_functions/iceberg_snapshots.cpp
    src/iceberg_functions/iceberg_scan.cpp
    src/iceberg_functions/iceberg_metadata.cpp
    src/iceberg_functions/iceberg_table_properties_functions.cpp
    src/iceberg_functions/iceberg_to_ducklake.cpp
    src/storage/authorization/sigv4.cpp
    src/storage/authorization/none.cpp
    src/storage/authorization/oauth2.cpp
    src/storage/authorization/azure.cpp
    src/storage/iceberg_transaction_data.cpp
    src/storage/irc_authorization.cpp
    src/storage/irc_catalog.cpp
    src/storage/irc_schema_entry.cpp
    src/storage/irc_schema_set.cpp
    src/storage/irc_table_entry.cpp
    src/storage/irc_table_set.cpp
    src/storage/irc_transaction.cpp
    src/storage/irc_authorization.cpp
    src/storage/irc_transaction_manager.cpp
    src/utils/iceberg_type.cpp
    src/storage/table_update/iceberg_add_snapshot.cpp
    src/storage/table_update/common.cpp
    src/storage/iceberg_table_information.cpp
    src/storage/create_table/iceberg_create_table_request.cpp
)

add_subdirectory(src/rest_catalog/objects)

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES} ${ALL_OBJECT_FILES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES} ${ALL_OBJECT_FILES})

find_package(CURL REQUIRED)
find_package(AWSSDK REQUIRED COMPONENTS core sso sts)

# Azure SDK
find_package(azure-identity-cpp CONFIG REQUIRED)
find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
find_package(azure-storage-files-datalake-cpp CONFIG REQUIRED)
set(HAVE_AZURE_SDK TRUE)
add_compile_definitions(DUCKDB_ICEBERG_AZURE_SUPPORT=1)

include_directories(${CURL_INCLUDE_DIRS})

# Roaring is installed via vcpkg and used here
find_package(roaring CONFIG REQUIRED)

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

# Azure SDK FROM vcpkg
target_link_libraries(${EXTENSION_NAME} PUBLIC Azure::azure-identity Azure::azure-storage-blobs Azure::azure-storage-files-datalake)
target_link_libraries(${TARGET_NAME}_loadable_extension Azure::azure-identity Azure::azure-storage-blobs Azure::azure-storage-files-datalake)

# Link dependencies into extension
target_link_libraries(${EXTENSION_NAME} PUBLIC ${CURL_LIBRARIES})
target_link_libraries(${TARGET_NAME}_loadable_extension ${CURL_LIBRARIES})

# Link Roaring library
target_link_libraries(${EXTENSION_NAME} PUBLIC roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)
target_link_libraries(${TARGET_NAME}_loadable_extension roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)

install(
  TARGETS ${EXTENSION_NAME} ${TARGET_NAME}_loadable_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
