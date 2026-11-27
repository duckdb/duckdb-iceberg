add_library(iceberg_metrics OBJECT iceberg_profile_metrics.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_metrics>
    PARENT_SCOPE)
