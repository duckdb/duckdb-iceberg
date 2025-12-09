add_subdirectory(common)
add_subdirectory(deletes)
add_subdirectory(metadata)
add_subdirectory(iceberg_functions)
add_subdirectory(utils)
add_subdirectory(storage)

# Top-level src/ translation units (those living directly under src/)
add_library(
  iceberg_src_objects OBJECT
  iceberg_extension.cpp
  iceberg_functions.cpp
  iceberg_manifest.cpp
  iceberg_manifest_list.cpp
  avro_scan.cpp
  iceberg_snapshot_lookup.cpp
  catalog_api.cpp
  iceberg_logging.cpp
  catalog_utils.cpp
  aws.cpp
  hash_utils.cpp
  base_manifest_reader.cpp
  manifest_list_reader.cpp
  manifest_file_reader.cpp
  iceberg_predicate.cpp
  iceberg_value.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_src_objects>
    PARENT_SCOPE)
