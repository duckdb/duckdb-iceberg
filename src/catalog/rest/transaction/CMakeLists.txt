add_library(
  iceberg_catalog_rest_transaction OBJECT
  iceberg_transaction.cpp iceberg_transaction_data.cpp
  iceberg_transaction_manager.cpp iceberg_transaction_update.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog_rest_transaction>
    PARENT_SCOPE)
