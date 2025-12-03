cmake_minimum_required(VERSION 3.5...3.29)

# Set extension name here
set(TARGET_NAME iceberg)
project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 14 CACHE STRING "C++ standard")
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
include_directories(src/include)

add_subdirectory(src)
set(EXTENSION_SOURCES ${ALL_OBJECT_FILES})

add_subdirectory(src/rest_catalog/objects)

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES} ${ALL_OBJECT_FILES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES} ${ALL_OBJECT_FILES})

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    find_package(CURL REQUIRED)
    find_package(AWSSDK REQUIRED COMPONENTS core sso sts)
    include_directories(${CURL_INCLUDE_DIRS})
endif()

# Roaring is installed via vcpkg and used here
find_package(roaring CONFIG REQUIRED)

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

install(
  TARGETS ${EXTENSION_NAME} ${TARGET_NAME}_loadable_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
