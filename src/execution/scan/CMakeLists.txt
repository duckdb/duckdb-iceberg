add_library(iceberg_execution_scan OBJECT iceberg_task_executor.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_execution_scan>
    PARENT_SCOPE)
