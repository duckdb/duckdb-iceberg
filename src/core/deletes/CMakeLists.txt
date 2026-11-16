add_library(iceberg_core_deletes OBJECT iceberg_deletion_vector.cpp
                                        iceberg_positional_delete.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_core_deletes>
    PARENT_SCOPE)
