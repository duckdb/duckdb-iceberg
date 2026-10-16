add_library(iceberg_function_scan OBJECT iceberg_scan.cpp
                                         iceberg_scan_snapshot.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_function_scan>
    PARENT_SCOPE)
