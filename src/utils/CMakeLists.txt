add_library(iceberg_utils_objects OBJECT iceberg_type.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_utils_objects>
    PARENT_SCOPE)
