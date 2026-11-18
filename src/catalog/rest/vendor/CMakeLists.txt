add_library(iceberg_catalog_rest_vendor OBJECT
            iceberg_glue_rest_backend.cpp iceberg_s3tables_rest_backend.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog_rest_vendor>
    PARENT_SCOPE)
