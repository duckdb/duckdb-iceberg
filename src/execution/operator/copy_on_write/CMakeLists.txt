add_library(iceberg_execution_operator_copy_on_write OBJECT
            iceberg_copy_on_write_plan.cpp iceberg_copy_on_write_operators.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    $<TARGET_OBJECTS:iceberg_execution_operator_copy_on_write>
    PARENT_SCOPE)
