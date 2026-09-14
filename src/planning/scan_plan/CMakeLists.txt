add_library(
  iceberg_planning_scan_plan OBJECT
  iceberg_client_scan_plan_provider.cpp iceberg_scan_plan_provider.cpp
  iceberg_scan_plan_state.cpp iceberg_scan_planner.cpp
  iceberg_server_scan_plan_provider.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_planning_scan_plan>
    PARENT_SCOPE)
