add_library(iceberg_catalog_transaction OBJECT iceberg_commit_plan.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog_transaction>
    PARENT_SCOPE)
