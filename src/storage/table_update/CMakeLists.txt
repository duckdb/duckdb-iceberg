add_library(iceberg_storage_table_update_objects OBJECT iceberg_add_snapshot.cpp
                                                        common.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_storage_table_update_objects>
    PARENT_SCOPE)
