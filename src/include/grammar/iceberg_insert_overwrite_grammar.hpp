#pragma once

namespace duckdb {
class DatabaseInstance;

//! Registers the grammar extension 'iceberg_insert_overwrite' (INSERT OVERWRITE for Iceberg tables, off until it is
//! listed in 'active_grammar_extensions') and the planner hooks that carry the overwrite to the insert operator
void RegisterIcebergInsertOverwrite(DatabaseInstance &db);

} // namespace duckdb
