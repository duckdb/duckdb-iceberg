add_library(
  iceberg_functions_objects OBJECT
  iceberg_multi_file_reader.cpp
  iceberg_avro_multi_file_reader.cpp
  iceberg_deletes_file_reader.cpp
  iceberg_multi_file_list.cpp
  iceberg_snapshots.cpp
  iceberg_scan.cpp
  iceberg_metadata.cpp
  iceberg_table_properties_functions.cpp
  iceberg_to_ducklake.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_functions_objects>
    PARENT_SCOPE)
