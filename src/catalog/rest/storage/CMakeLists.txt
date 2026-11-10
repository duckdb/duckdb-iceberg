add_subdirectory(authorization)

add_library(iceberg_catalog_rest_storage OBJECT
            aws.cpp iceberg_table_secret_provider.cpp iceberg_authorization.cpp
            iceberg_remote_signing.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog_rest_storage>
    PARENT_SCOPE)
