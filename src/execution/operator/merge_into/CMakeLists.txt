add_library(iceberg_execution_operator_merge_into OBJECT iceberg_merge_into.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_execution_operator_merge_into>
    PARENT_SCOPE)
