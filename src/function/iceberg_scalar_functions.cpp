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
// iceberg_bucket(num_buckets, value) -> INTEGER
// Iceberg spec: bucket = (murmur3_hash(value) & 0x7FFFFFFF) % num_buckets
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> IcebergBucketBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 2);
	auto &num_buckets_expr = *arguments[0];
	if (num_buckets_expr.IsFoldable()) {
		auto num_buckets_val = ExpressionExecutor::EvaluateScalar(context, num_buckets_expr);
		if (!num_buckets_val.IsNull() && num_buckets_val.GetValue<int32_t>() <= 0) {
			throw InvalidInputException("iceberg_bucket: modulo must be a positive integer, got %d",
			                            num_buckets_val.GetValue<int32_t>());
		}
	}
	return nullptr;
}

static void IcebergBucketInteger(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int32_t n, int32_t val) -> int32_t { return (IcebergHash::HashInt32(val) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketBigInt(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, int64_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int32_t n, int64_t val) -> int32_t { return (IcebergHash::HashInt64(val) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketVarchar(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, string_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int32_t n, string_t val) -> int32_t { return (IcebergHash::HashString(val) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketBlob(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, string_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(), [](int32_t n, string_t val) -> int32_t {
		    int32_t h = IcebergHash::Murmur3Hash32(reinterpret_cast<const uint8_t *>(val.GetData()), val.GetSize(), 0);
		    return (h & 0x7FFFFFFF) % n;
	    });
}

static void IcebergBucketDate(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, date_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int32_t n, date_t val) -> int32_t { return (IcebergHash::HashDate(val) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketTimestamp(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, timestamp_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int32_t n, timestamp_t val) -> int32_t { return (IcebergHash::HashInt64(val.value) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketTimestampTz(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, timestamp_tz_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int32_t n, timestamp_tz_t val) -> int32_t { return (IcebergHash::HashInt64(val.value) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketTimestampNs(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, timestamp_ns_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int32_t n, timestamp_ns_t val) -> int32_t { return (IcebergHash::HashTimestampNs(val) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketTime(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, dtime_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int32_t n, dtime_t val) -> int32_t { return (IcebergHash::HashTime(val) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketUUID(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, hugeint_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int32_t n, hugeint_t val) -> int32_t { return (IcebergHash::HashUUID(val) & 0x7FFFFFFF) % n; });
}

static void IcebergBucketDecimalInt16(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, int16_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(), [](int32_t n, int16_t val) -> int32_t {
		    return (IcebergHash::HashDecimalInt64(static_cast<int64_t>(val)) & 0x7FFFFFFF) % n;
	    });
}

static void IcebergBucketDecimalInt32(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(), [](int32_t n, int32_t val) -> int32_t {
		    return (IcebergHash::HashDecimalInt64(static_cast<int64_t>(val)) & 0x7FFFFFFF) % n;
	    });
}

static void IcebergBucketDecimalInt64(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, int64_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(), [](int32_t n, int64_t val) -> int32_t {
		    return (IcebergHash::HashDecimalInt64(val) & 0x7FFFFFFF) % n;
	    });
}

static void IcebergBucketDecimalHugeInt(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, hugeint_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(), [](int32_t n, hugeint_t val) -> int32_t {
		    return (IcebergHash::HashDecimalHugeInt(val) & 0x7FFFFFFF) % n;
	    });
}

static unique_ptr<FunctionData> IcebergBucketDecimalBind(ClientContext &context, ScalarFunction &bound_function,
                                                         vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 2);
	auto &num_buckets_expr = *arguments[0];
	if (num_buckets_expr.IsFoldable()) {
		auto num_buckets_val = ExpressionExecutor::EvaluateScalar(context, num_buckets_expr);
		if (!num_buckets_val.IsNull() && num_buckets_val.GetValue<int32_t>() <= 0) {
			throw InvalidInputException("iceberg_bucket: modulo must be a positive integer, got %d",
			                            num_buckets_val.GetValue<int32_t>());
		}
	}
	auto &decimal_type = arguments[1]->return_type;
	bound_function.arguments[1] = decimal_type;
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		bound_function.SetFunctionCallback(IcebergBucketDecimalInt16);
		break;
	case PhysicalType::INT32:
		bound_function.SetFunctionCallback(IcebergBucketDecimalInt32);
		break;
	case PhysicalType::INT64:
		bound_function.SetFunctionCallback(IcebergBucketDecimalInt64);
		break;
	default: // INT128
		bound_function.SetFunctionCallback(IcebergBucketDecimalHugeInt);
		break;
	}
	return nullptr;
}

ScalarFunctionSet IcebergFunctions::GetIcebergBucketFunction() {
	ScalarFunctionSet set("iceberg_bucket");
	// (num_buckets, value) -> INTEGER
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER,
	                               IcebergBucketInteger, IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::BIGINT}, LogicalType::INTEGER,
	                               IcebergBucketBigInt, IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::INTEGER,
	                               IcebergBucketVarchar, IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::BLOB}, LogicalType::INTEGER, IcebergBucketBlob,
	                               IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::DATE}, LogicalType::INTEGER, IcebergBucketDate,
	                               IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::TIMESTAMP}, LogicalType::INTEGER,
	                               IcebergBucketTimestamp, IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::TIMESTAMP_TZ}, LogicalType::INTEGER,
	                               IcebergBucketTimestampTz, IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::TIMESTAMP_NS}, LogicalType::INTEGER,
	                               IcebergBucketTimestampNs, IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::TIME}, LogicalType::INTEGER, IcebergBucketTime,
	                               IcebergBucketBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::UUID}, LogicalType::INTEGER, IcebergBucketUUID,
	                               IcebergBucketBind));
	{
		scalar_function_t null_fn;
		set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType(LogicalTypeId::DECIMAL)},
		                               LogicalType::INTEGER, null_fn, IcebergBucketDecimalBind));
	}
	return set;
}

//===--------------------------------------------------------------------===//
// iceberg_truncate(width, value) -> same type as value
// Iceberg spec:
//   integers:  v - (((v % W) + W) % W)      (floor to nearest multiple of W)
//   strings:   first L grapheme clusters
//   binary:    first L bytes
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> IcebergTruncateBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 2);
	auto &width_expr = *arguments[0];
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
	    [](int32_t W, int32_t v) -> int32_t { return v - (((v % W) + W) % W); });
}

static void IcebergTruncateBigInt(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, int64_t, int64_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [](int32_t W, int64_t v) -> int64_t { return v - (((v % W) + W) % W); });
}

static void IcebergTruncateVarchar(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, string_t, string_t>(
	    input.data[0], input.data[1], result, input.size(), [&result](int32_t L, string_t val) -> string_t {
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
	BinaryExecutor::Execute<int32_t, string_t, string_t>(
	    input.data[0], input.data[1], result, input.size(), [&result](int32_t L, string_t val) -> string_t {
		    auto size = val.GetSize();
		    auto truncated = static_cast<idx_t>(L) < size ? static_cast<idx_t>(L) : size;
		    return StringVector::AddStringOrBlob(result, val.GetData(), truncated);
	    });
}

static void IcebergTruncateDecimalInt16(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, int16_t, int16_t>(
	    input.data[0], input.data[1], result, input.size(), [](int32_t W, int16_t v) -> int16_t {
		    int64_t val = static_cast<int64_t>(v);
		    int64_t w = static_cast<int64_t>(W);
		    return static_cast<int16_t>(val - (((val % w) + w) % w));
	    });
}

static void IcebergTruncateDecimalInt32(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, int32_t, int32_t>(
	    input.data[0], input.data[1], result, input.size(), [](int32_t W, int32_t v) -> int32_t {
		    int64_t val = static_cast<int64_t>(v);
		    int64_t w = static_cast<int64_t>(W);
		    return static_cast<int32_t>(val - (((val % w) + w) % w));
	    });
}

static void IcebergTruncateDecimalInt64(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, int64_t, int64_t>(
	    input.data[0], input.data[1], result, input.size(), [](int32_t W, int64_t v) -> int64_t {
		    int64_t w = static_cast<int64_t>(W);
		    return v - (((v % w) + w) % w);
	    });
}

static void IcebergTruncateDecimalHugeInt(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<int32_t, hugeint_t, hugeint_t>(
	    input.data[0], input.data[1], result, input.size(), [](int32_t W, hugeint_t v) -> hugeint_t {
		    hugeint_t w = hugeint_t(W);
		    return v - (((v % w) + w) % w);
	    });
}

static unique_ptr<FunctionData> IcebergTruncateDecimalBind(ClientContext &context, ScalarFunction &bound_function,
                                                           vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 2);
	auto &width_expr = *arguments[0];
	if (width_expr.IsFoldable()) {
		auto width_val = ExpressionExecutor::EvaluateScalar(context, width_expr);
		if (!width_val.IsNull() && width_val.GetValue<int32_t>() <= 0) {
			throw InvalidInputException("iceberg_truncate: width must be a positive integer, got %d",
			                            width_val.GetValue<int32_t>());
		}
	}
	auto &decimal_type = arguments[1]->return_type;
	bound_function.arguments[1] = decimal_type;
	bound_function.SetReturnType(decimal_type);
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		bound_function.SetFunctionCallback(IcebergTruncateDecimalInt16);
		break;
	case PhysicalType::INT32:
		bound_function.SetFunctionCallback(IcebergTruncateDecimalInt32);
		break;
	case PhysicalType::INT64:
		bound_function.SetFunctionCallback(IcebergTruncateDecimalInt64);
		break;
	default: // INT128
		bound_function.SetFunctionCallback(IcebergTruncateDecimalHugeInt);
		break;
	}
	return nullptr;
}

ScalarFunctionSet IcebergFunctions::GetIcebergTruncateFunction() {
	ScalarFunctionSet set("iceberg_truncate");
	// (width, value) -> same type as value
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER,
	                               IcebergTruncateInteger, IcebergTruncateBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::BIGINT}, LogicalType::BIGINT,
	                               IcebergTruncateBigInt, IcebergTruncateBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               IcebergTruncateVarchar, IcebergTruncateBind));
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::BLOB}, LogicalType::BLOB, IcebergTruncateBlob,
	                               IcebergTruncateBind));
	{
		scalar_function_t null_fn;
		set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType(LogicalTypeId::DECIMAL)},
		                               LogicalType(LogicalTypeId::DECIMAL), null_fn, IcebergTruncateDecimalBind));
	}
	return set;
}

} // namespace duckdb
