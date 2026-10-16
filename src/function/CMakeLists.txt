add_subdirectory(copy)
add_subdirectory(ducklake)
add_subdirectory(metadata)
add_subdirectory(scan)
add_subdirectory(scan_planning)

add_library(iceberg_function OBJECT iceberg_functions.cpp
                                    iceberg_scalar_functions.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_function>
    PARENT_SCOPE)
