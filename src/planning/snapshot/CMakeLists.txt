add_library(iceberg_planning_snapshot OBJECT iceberg_incremental_scan.cpp
                                             iceberg_snapshot_lookup.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_planning_snapshot>
    PARENT_SCOPE)
