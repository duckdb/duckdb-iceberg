add_library(
  iceberg_planning_metadata_io_avro OBJECT
  avro_scan.cpp iceberg_manifest_cache.cpp iceberg_avro_multi_file_list.cpp
  iceberg_avro_multi_file_reader.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_planning_metadata_io_avro>
    PARENT_SCOPE)
