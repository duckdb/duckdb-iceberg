# CMakeLists.txt

# DuckDB CMake configuration file

project(duckdb)

# Add source files

set(SOURCES
    src/common/exception.cpp
    src/common/types.cpp
    src/common/transaction.cpp
    src/function/function.cpp
    src/planner/planner.cpp
    src/storage/storage.cpp
    src/storage/storage_manager.cpp
    src/storage/transaction_manager.cpp
    src/common/persistent_table.cpp
    src/storage/table_factory.cpp
    src/main/duckdb.cpp
    src/optimizer/optimizer.cpp
    src/storage/storage_util.cpp
    src/storage/authorization/oauth2.cpp
    src/storage/authorization/azure.cpp  # Added azure.cpp
    # More sources...
)

set(EXTENSION_SOURCES
    ${SOURCES}
    # Add additional extension sources here...
)
