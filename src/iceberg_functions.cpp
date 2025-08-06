#include "iceberg_functions.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

class ExtensionLoader;

namespace duckdb {
vector<TableFunctionSet> IcebergFunctions::GetTableFunctions(ExtensionLoader &loader) {
	vector<TableFunctionSet> functions;

	functions.push_back(std::move(GetIcebergSnapshotsFunction()));
	functions.push_back(std::move(GetIcebergScanFunction(loader)));
	functions.push_back(std::move(GetIcebergMetadataFunction()));
	functions.push_back(std::move(GetIcebergToDuckLakeFunction()));

	return functions;
}

vector<ScalarFunction> IcebergFunctions::GetScalarFunctions() {
	vector<ScalarFunction> functions;

	return functions;
}

} // namespace duckdb
