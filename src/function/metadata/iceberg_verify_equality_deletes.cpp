#include "duckdb/common/file_system.hpp"

#include "function/iceberg_functions.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"

#include <numeric>

namespace duckdb {

static void IcebergVerifyEqualityDeletesFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
	ConstantVector::GetData<bool>(result)[0] = true;
	result.SetValue(0, Value::BOOLEAN(true));
}

ScalarFunctionSet IcebergFunctions::GetVerifyEqualityDeletesFunction() {
	ScalarFunctionSet function_set("iceberg_verify_equality_deletes");

	ScalarFunction fun({}, LogicalType::BOOLEAN, IcebergVerifyEqualityDeletesFunction);
	// is this a variable length argument then?
	fun.SetVarArgs(LogicalType::ANY);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	// set to volatile so it does not get pushed down into the scan
	fun.SetVolatile();
	function_set.AddFunction(fun);
	return function_set;
}

} // namespace duckdb
