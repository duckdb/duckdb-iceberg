add_library(iceberg_grammar OBJECT iceberg_insert_overwrite_grammar.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_grammar>
    PARENT_SCOPE)
