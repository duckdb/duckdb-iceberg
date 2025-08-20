//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

class IcebergFunctions {
public:
	static vector<TableFunctionSet> GetTableFunctions(DatabaseInstance &instance);
	static vector<ScalarFunction> GetScalarFunctions();

private:
	static TableFunctionSet GetIcebergSnapshotsFunction();
	static TableFunctionSet GetIcebergScanFunction(DatabaseInstance &instance);
	static TableFunctionSet GetIcebergMetadataFunction();
	static TableFunctionSet GetIcebergToDuckLakeFunction();
};

} // namespace duckdb
