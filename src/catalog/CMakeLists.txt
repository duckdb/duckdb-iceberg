add_subdirectory(rest)
add_subdirectory(transaction)

add_library(iceberg_catalog OBJECT iceberg_catalog_backend.cpp
                                   iceberg_catalog_types.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog>
    PARENT_SCOPE)
