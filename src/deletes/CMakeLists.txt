add_library(iceberg_deletes_objects OBJECT
    equality_delete.cpp
    positional_delete.cpp
    deletion_vector.cpp
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_deletes_objects>
    PARENT_SCOPE)