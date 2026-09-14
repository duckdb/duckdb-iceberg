add_library(
  iceberg_maintenance OBJECT
  expire_snapshots_operator.cpp expire_snapshots_planner.cpp
  maintenance_table_loader.cpp rewrite_data_files_executor.cpp
  rewrite_data_files_operator.cpp rewrite_data_files_planner.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_maintenance>
    PARENT_SCOPE)
