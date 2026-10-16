add_library(iceberg_core_metadata_partition OBJECT
            iceberg_partition_spec.cpp iceberg_partition_constants.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_core_metadata_partition>
    PARENT_SCOPE)
