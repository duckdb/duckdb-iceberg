add_library(
  iceberg_planning_deletes OBJECT
  iceberg_delete_file_scanner.cpp iceberg_delete_planner.cpp
  iceberg_equality_delete_fast_filter.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_planning_deletes>
    PARENT_SCOPE)
