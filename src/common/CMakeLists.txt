add_library(iceberg_common_objects OBJECT utils.cpp url_utils.cpp iceberg.cpp
                                          api_utils.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_common_objects>
    PARENT_SCOPE)
