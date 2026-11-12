add_subdirectory(deletes)
add_subdirectory(metadata_io)
add_subdirectory(pruning)
add_subdirectory(scan_order)
add_subdirectory(scan_plan)
add_subdirectory(snapshot)

add_library(
  iceberg_planning OBJECT
  iceberg_manifest_read_state.cpp iceberg_multi_file_list.cpp
  iceberg_multi_file_reader.cpp iceberg_planner.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_planning>
    PARENT_SCOPE)
