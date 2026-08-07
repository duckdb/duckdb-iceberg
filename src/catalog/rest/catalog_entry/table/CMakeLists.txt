add_library(iceberg_catalog_rest_catalog_entry_table OBJECT
            iceberg_table_schema_version.cpp iceberg_table.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    $<TARGET_OBJECTS:iceberg_catalog_rest_catalog_entry_table>
    PARENT_SCOPE)
