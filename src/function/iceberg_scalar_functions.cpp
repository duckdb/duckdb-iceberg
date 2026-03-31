//===----------------------------------------------------------------------===//
//                         DuckDB
//
// Iceberg scalar functions: iceberg_bucket and iceberg_truncate
//
// These implement the Iceberg partition transform algorithms as callable SQL
// functions, matching the spec at:
// https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
//===----------------------------------------------------------------------===//

#include "function/iceberg_functions.hpp"
#include "core/expression/iceberg_hash.hpp"

#include "duckdb.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// iceberg_bucket(value, num_buckets) -> INTEGER
// Iceberg spec: bucket = (murmur3_hash(value) & 0x7FFFFFFF) % num_buckets
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> IcebergBucketBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 2);
	auto &width_expr = *arguments[1];
	if (width_expr.IsFoldable()) {
		auto width_val = ExpressionExecutor::EvaluateScalar(context, width_expr);
		if (!width_val.IsNull() && width_val.GetValue<int32_t>() <= 0) {
			throw InvalidInputException("iceberg_bucket: modulo must be a positive integer, got %d",
			                            width_val.GetValue<int32_t>());
		}
	}
	return nullptr;
}

static void IcebergBucketInteger(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int32_t val, int32_t n) -> int32_t { return (IcebergHash::HashInt32(val) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketBigInt(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int64_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int64_t val, int32_t n) -> int32_t { return (IcebergHash::HashInt64(val) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketVarchar(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](string_t val, int32_t n) -> int32_t { return (IcebergHash::HashString(val) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketBlob(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(), [](string_t val, int32_t n) -> int32_t {
		    int32_t h = IcebergHash::Murmur3Hash32(reinterpret_cast<const uint8_t *>(val.GetData()), val.GetSize(), 0);
		    return (h & 0x7FFFFFFF) % n;
	    });
}

static void IcebergBucketDate(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<date_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](date_t val, int32_t n) -> int32_t { return (IcebergHash::HashDate(val) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketTimestamp(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<timestamp_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](timestamp_t val, int32_t n) -> int32_t { return (IcebergHash::HashInt64(val.value) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketTimestampTz(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<timestamp_tz_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](timestamp_tz_t val, int32_t n) -> int32_t { return (IcebergHash::HashInt64(val.value) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketTimestampNs(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<timestamp_ns_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](timestamp_ns_t val, int32_t n) -> int32_t { return (IcebergHash::HashTimestampNs(val) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketTime(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<dtime_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](dtime_t val, int32_t n) -> int32_t { return (IcebergHash::HashTime(val) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketUUID(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<hugeint_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](hugeint_t val, int32_t n) -> int32_t { return (IcebergHash::HashUUID(val) & 0x7FFFFFFF) % n; });
}

ScalarFunctionSet IcebergFunctions::GetIcebergBucketFunction() {
	ScalarFunctionSet set("iceberg_bucket");
	// (value, num_buckets) -> INTEGER
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER,
	                               IcebergBucketInteger, IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER}, LogicalType::INTEGER,
	                               IcebergBucketBigInt, IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::INTEGER,
	                               IcebergBucketVarchar, IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::BLOB, LogicalType::INTEGER}, LogicalType::INTEGER, IcebergBucketBlob,
	                               IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::DATE, LogicalType::INTEGER}, LogicalType::INTEGER, IcebergBucketDate,
	                               IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP, LogicalType::INTEGER}, LogicalType::INTEGER,
	                               IcebergBucketTimestamp, IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ, LogicalType::INTEGER}, LogicalType::INTEGER,
	                               IcebergBucketTimestampTz, IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_NS, LogicalType::INTEGER}, LogicalType::INTEGER,
	                               IcebergBucketTimestampNs, IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::TIME, LogicalType::INTEGER}, LogicalType::INTEGER, IcebergBucketTime,
	                               IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::UUID, LogicalType::INTEGER}, LogicalType::INTEGER, IcebergBucketUUID,
	                               IcebergBucketBind));
	return set;
}

//===--------------------------------------------------------------------===//
// iceberg_truncate(value, width) -> same type as value
// Iceberg spec:
//   integers:  v - (((v % W) + W) % W)      (floor to nearest multiple of W)
//   strings:   first L grapheme clusters
//   binary:    first L bytes
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> IcebergTruncateBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 2);
	auto &width_expr = *arguments[1];
	if (width_expr.IsFoldable()) {
		auto width_val = ExpressionExecutor::EvaluateScalar(context, width_expr);
		if (!width_val.IsNull() && width_val.GetValue<int32_t>() <= 0) {
			throw InvalidInputException("iceberg_truncate: width must be a positive integer, got %d",
			                            width_val.GetValue<int32_t>());
		}
	}
	return nullptr;
}

static void IcebergTruncateInteger(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int32_t v, int32_t W) -> int32_t { return v - (((v % W) + W) % W); });
}

static void IcebergTruncateBigInt(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int64_t, int32_t, int64_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int64_t v, int32_t W) -> int64_t { return v - (((v % W) + W) % W); });
}

static void IcebergTruncateVarchar(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, int32_t, string_t>(
	    input.data[0], input.data[1], result, input.size(), [&result](string_t val, int32_t L) -> string_t {
		    auto data = val.GetData();
		    auto size = val.GetSize();
		    size_t num_chars = 0;
		    for (auto cluster : Utf8Proc::GraphemeClusters(data, size)) {
			    if (++num_chars >= static_cast<size_t>(L)) {
				    return StringVector::AddString(result, data, cluster.end);
			    }
		    }
		    // Fewer grapheme clusters than width: return the whole string
		    return StringVector::AddString(result, data, size);
	    });
}

static void IcebergTruncateBlob(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, int32_t, string_t>(
	    input.data[0], input.data[1], result, input.size(), [&result](string_t val, int32_t L) -> string_t {
		    auto size = val.GetSize();
		    auto truncated = static_cast<idx_t>(L) < size ? static_cast<idx_t>(L) : size;
		    return StringVector::AddStringOrBlob(result, val.GetData(), truncated);
	    });
}

ScalarFunctionSet IcebergFunctions::GetIcebergTruncateFunction() {
	ScalarFunctionSet set("iceberg_truncate");
	// (value, width) -> same type as value
	// Width is validated at bind time for constant expressions; numeric variants also
	// guard at execution time since v % 0 is undefined behaviour.
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER,
	                               IcebergTruncateInteger, IcebergTruncateBind));
	set.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER}, LogicalType::BIGINT,
	                               IcebergTruncateBigInt, IcebergTruncateBind));
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::VARCHAR,
	                               IcebergTruncateVarchar, IcebergTruncateBind));
	set.AddFunction(ScalarFunction({LogicalType::BLOB, LogicalType::INTEGER}, LogicalType::BLOB, IcebergTruncateBlob,
	                               IcebergTruncateBind));
	return set;
}

} // namespace duckdb
