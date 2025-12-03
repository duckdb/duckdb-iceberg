add_subdirectory(authorization)
add_subdirectory(table_update)
add_subdirectory(create_table)

add_library(iceberg_storage_objects OBJECT
    iceberg_insert.cpp
    iceberg_delete.cpp
    iceberg_update.cpp
    iceberg_transaction_data.cpp
    irc_authorization.cpp
    irc_catalog.cpp
    irc_schema_entry.cpp
    irc_schema_set.cpp
    irc_table_entry.cpp
    irc_table_set.cpp
    irc_transaction.cpp
    irc_transaction_manager.cpp
    iceberg_table_information.cpp
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_storage_objects>
    PARENT_SCOPE)