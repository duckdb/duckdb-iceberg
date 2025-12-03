add_library(iceberg_storage_auth_objects OBJECT
    sigv4.cpp
    none.cpp
    oauth2.cpp
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_storage_auth_objects>
    PARENT_SCOPE)