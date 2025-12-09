cmake_minimum_required(VERSION 3.5...3.29)

# Set extension name here
set(TARGET_NAME iceberg)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

set(CMAKE_CXX_STANDARD 14 CACHE STRING "C++ standard")
set(CMAKE_CXX_STANDARD_REQUIRED True)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    find_package(CURL REQUIRED)
    find_package(AWSSDK REQUIRED COMPONENTS core sso sts)
    include_directories(${CURL_INCLUDE_DIRS})
endif()

# Roaring is installed via vcpkg and used here
find_package(roaring CONFIG REQUIRED)

# Project-wide public include directory (use absolute path for clarity in IDEs)
set(PROJECT_INC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/include)

add_subdirectory(src)
set(EXTENSION_SOURCES ${ALL_OBJECT_FILES})

set(ICEBERG_OBJECT_LIBS
        iceberg_src_objects
        iceberg_common_objects
        iceberg_deletes_objects
        iceberg_metadata_objects
        iceberg_functions_objects
        iceberg_utils_objects
        iceberg_storage_objects
        iceberg_storage_auth_objects
        iceberg_storage_table_update_objects
        iceberg_storage_create_table_objects
        rest_catalog_objects
)

foreach(tgt IN LISTS ICEBERG_OBJECT_LIBS)
    if(TARGET ${tgt})
        if(NOT CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
            # AWSSDK variables come from find_package(AWSSDK ...)
            if(DEFINED AWSSDK_LINK_LIBRARIES)
                target_link_libraries(${tgt} PUBLIC ${AWSSDK_LINK_LIBRARIES})
            endif()
            if(DEFINED CURL_LIBRARIES)
                target_link_libraries(${tgt} PUBLIC ${CURL_LIBRARIES})
            endif()
        endif()

        # Link roaring imported targets (these are imported targets provided by the package)
        # This will propagate include directories for <roaring/roaring.hh>
        target_link_libraries(${tgt} PUBLIC roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)

        # Ensure sources in these OBJECT libraries see headers in src/include
        target_include_directories(${tgt} PUBLIC ${PROJECT_INC_DIR})
    endif()
endforeach()

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES} ${ALL_OBJECT_FILES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES} ${ALL_OBJECT_FILES})


# Reset the TARGET_NAME, the AWS find_package build could bleed into our build -
# overriding `TARGET_NAME`
set(TARGET_NAME iceberg)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    # AWS SDK FROM vcpkg
    target_include_directories(${EXTENSION_NAME}
                            PUBLIC $<BUILD_INTERFACE:${AWSSDK_INCLUDE_DIRS}>)
    target_link_libraries(${EXTENSION_NAME} PUBLIC ${AWSSDK_LINK_LIBRARIES})
    target_include_directories(${TARGET_NAME}_loadable_extension
                            PRIVATE $<BUILD_INTERFACE:${AWSSDK_INCLUDE_DIRS}>)
    target_link_libraries(${TARGET_NAME}_loadable_extension
                        ${AWSSDK_LINK_LIBRARIES})

    # Link dependencies into extension
    target_link_libraries(${EXTENSION_NAME} PUBLIC ${CURL_LIBRARIES})
    target_link_libraries(${TARGET_NAME}_loadable_extension ${CURL_LIBRARIES})
endif()

# Link Roaring library
target_link_libraries(${EXTENSION_NAME} PUBLIC roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)
target_link_libraries(${TARGET_NAME}_loadable_extension roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)

# Attach the project include directory to the final targets so IDEs index correctly
target_include_directories(${EXTENSION_NAME} PUBLIC ${PROJECT_INC_DIR})
target_include_directories(${TARGET_NAME}_loadable_extension PUBLIC ${PROJECT_INC_DIR})

install(
  TARGETS ${EXTENSION_NAME} ${TARGET_NAME}_loadable_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
