add_library(iceberg_common OBJECT iceberg_default.cpp iceberg_file_cleanup.cpp
                                  iceberg_fixed_type.cpp iceberg_utils.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_common>
    PARENT_SCOPE)
