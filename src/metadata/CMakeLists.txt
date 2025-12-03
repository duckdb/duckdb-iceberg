add_library(iceberg_metadata_objects OBJECT
    iceberg_transform.cpp
    iceberg_table_schema.cpp
    iceberg_partition_spec.cpp
    iceberg_sort_order.cpp
    iceberg_snapshot.cpp
    iceberg_field_mapping.cpp
    iceberg_column_definition.cpp
    iceberg_table_metadata.cpp
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_metadata_objects>
    PARENT_SCOPE)