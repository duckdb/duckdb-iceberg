add_library(
  iceberg_function_metadata OBJECT
  iceberg_column_stats.cpp
  iceberg_load_table_response.cpp
  iceberg_metadata.cpp
  iceberg_partition_stats.cpp
  iceberg_rewrite_data_files.cpp
  iceberg_rollback_to_snapshot.cpp
  iceberg_schema_properties_functions.cpp
  iceberg_snapshots.cpp
  iceberg_table_properties_functions.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_function_metadata>
    PARENT_SCOPE)
