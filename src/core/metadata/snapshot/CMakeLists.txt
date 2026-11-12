add_library(
  iceberg_core_metadata_snapshot OBJECT
  iceberg_row_id_allocator.cpp iceberg_snapshot.cpp
  iceberg_snapshot_metrics.cpp iceberg_snapshot_writer.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_core_metadata_snapshot>
    PARENT_SCOPE)
