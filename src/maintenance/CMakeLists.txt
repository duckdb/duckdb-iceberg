add_library(
  iceberg_maintenance OBJECT
  maintenance_table_loader.cpp rewrite_data_files_executor.cpp
  rewrite_data_files_operator.cpp rewrite_data_files_planner.cpp
  rewrite_manifests.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_maintenance>
    PARENT_SCOPE)
