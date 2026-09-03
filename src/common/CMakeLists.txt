add_library(iceberg_common OBJECT iceberg_default.cpp iceberg_math.cpp
                                  iceberg_utils.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_common>
    PARENT_SCOPE)
