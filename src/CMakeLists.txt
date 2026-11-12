add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(core)
add_subdirectory(execution)
add_subdirectory(function)
add_subdirectory(maintenance)
add_subdirectory(planning)
add_subdirectory(rest_catalog)
add_subdirectory(storage)

add_library(
  iceberg_src OBJECT
  iceberg_attach.cpp iceberg_attach_options.cpp iceberg_extension.cpp
  iceberg_logging.cpp iceberg_options.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_src>
    PARENT_SCOPE)
