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
class ExtensionLoader;

class IcebergFunctions {
public:
	static vector<TableFunctionSet> GetTableFunctions(ExtensionLoader &loader);
	static vector<ScalarFunctionSet> GetScalarFunctions();
	static TableFunctionSet GetIcebergDeletesScanFunction(ClientContext &context);

private:
	static ScalarFunctionSet GetIcebergBucketFunction();
	static ScalarFunctionSet GetIcebergTruncateFunction();

	static TableFunctionSet GetIcebergSnapshotsFunction();
	static TableFunctionSet GetIcebergScanFunction(ExtensionLoader &loader);
	static TableFunctionSet GetIcebergMetadataFunction();
	static TableFunctionSet GetIcebergLoadTableResponseFunction();
	static TableFunctionSet GetIcebergColumnStatsFunction();
	static TableFunctionSet GetIcebergPartitionStatsFunction();
	static TableFunctionSet GetIcebergToDuckLakeFunction();
	static TableFunctionSet GetIcebergTablePropertiesFunctions();
	static TableFunctionSet SetIcebergTablePropertiesFunctions();
	static TableFunctionSet RemoveIcebergTablePropertiesFunctions();
};

} // namespace duckdb
