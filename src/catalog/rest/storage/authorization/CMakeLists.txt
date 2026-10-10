add_library(iceberg_catalog_rest_storage_authorization OBJECT
            azure.cpp none.cpp oauth2.cpp sigv4.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    $<TARGET_OBJECTS:iceberg_catalog_rest_storage_authorization>
    PARENT_SCOPE)
