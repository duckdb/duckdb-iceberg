add_library(iceberg_storage_create_table_objects OBJECT
    iceberg_create_table_request.cpp
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_storage_create_table_objects>
    PARENT_SCOPE)