#include "core/expression/iceberg_transform.hpp"

#include "duckdb/common/string_util.hpp"
#include "utf8proc_wrapper.hpp"
#include "core/expression/iceberg_hash.hpp"

namespace duckdb {

IcebergTransform::IcebergTransform() : raw_transform() {
	type = IcebergTransformType::INVALID;
}

bool IcebergTransform::TransformFunctionSupported(const string &transform_name) {
	if (transform_name == "day" || transform_name == "month" || transform_name == "year" || transform_name == "hour" ||
	    transform_name == "truncate" || transform_name == "bucket") {
		return true;
	}
	return false;
}

IcebergTransform::IcebergTransform(const string &transform) : raw_transform(transform) {
	if (transform == "identity") {
		type = IcebergTransformType::IDENTITY;
	} else if (StringUtil::StartsWith(transform, "bucket")) {
		type = IcebergTransformType::BUCKET;
		D_ASSERT(transform[6] == '[');
		D_ASSERT(transform.back() == ']');
		auto start = transform.find('[');
		auto end = transform.rfind(']');
		auto digits = transform.substr(start + 1, end - start);
		modulo = std::stoi(digits);
	} else if (StringUtil::StartsWith(transform, "truncate")) {
		type = IcebergTransformType::TRUNCATE;
		D_ASSERT(transform[8] == '[');
		D_ASSERT(transform.back() == ']');
		auto start = transform.find('[');
		auto end = transform.rfind(']');
		auto digits = transform.substr(start + 1, end - start);
		width = std::stoi(digits);
	} else if (transform == "year") {
		type = IcebergTransformType::YEAR;
	} else if (transform == "month") {
		type = IcebergTransformType::MONTH;
	} else if (transform == "day") {
		type = IcebergTransformType::DAY;
	} else if (transform == "hour") {
		type = IcebergTransformType::HOUR;
	} else if (transform == "void") {
		type = IcebergTransformType::VOID;
	} else {
		throw NotImplementedException("Unrecognized transform ('%s')", transform);
	}
}

LogicalType IcebergTransform::GetBoundsType(const LogicalType &input) const {
	switch (type) {
	case IcebergTransformType::IDENTITY: {
		//! Appendix A: Avro Data Type Mappings
		//! The avro reader return will return the correct identity types now
		return input;
	}
	case IcebergTransformType::BUCKET:
		return LogicalType::INTEGER;
	case IcebergTransformType::TRUNCATE:
		return input;
	case IcebergTransformType::DAY:
	case IcebergTransformType::YEAR:
	case IcebergTransformType::MONTH:
	case IcebergTransformType::HOUR:
		return LogicalType::INTEGER;
	case IcebergTransformType::VOID:
		return input;
	default:
		throw InvalidConfigurationException("Can't produce a result type for transform %s and input type %s",
		                                    raw_transform, input.ToString());
	}
}

LogicalType IcebergTransform::GetSerializedType(const LogicalType &input) const {
	switch (type) {
	case IcebergTransformType::IDENTITY:
		return input;
	case IcebergTransformType::TRUNCATE:
		return input;
	case IcebergTransformType::BUCKET:
	case IcebergTransformType::YEAR:
	case IcebergTransformType::MONTH:
	case IcebergTransformType::HOUR:
	case IcebergTransformType::DAY:
		return LogicalType::INTEGER;
	case IcebergTransformType::VOID:
		return input;
	default:
		throw InvalidConfigurationException("Can't produce a result type for transform %s and input type %s",
		                                    raw_transform, input.ToString());
	}
}

void IcebergTransform::SetBucketOrTruncateValue(idx_t value) {
	switch (type) {
	case IcebergTransformType::BUCKET:
		modulo = value;
		return;
	case IcebergTransformType::TRUNCATE:
		width = value;
		return;
	default:
		throw InvalidInputException("Cannot set bucket or modulo value for transform '%s'", raw_transform);
	}
}

Value BucketTransform::ApplyTransform(const Value &constant, const IcebergTransform &transform) {
	if (constant.IsNull()) {
		// Iceberg spec: "All transforms must return null for a null input value"
		return Value(LogicalType::INTEGER);
	}

	// Check if this type is supported for bucket pushdown.
	// Supported types: integer, long, decimal, date, timestamp, timestamptz, string, binary.
	auto type_id = constant.type().id();
	switch (type_id) {
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		break;
	default:
		// Unsupported type: return a null Value so CompareEqual skips filtering
		return Value(LogicalType::INTEGER);
	}

	auto num_buckets = static_cast<int32_t>(transform.GetBucketModulo());
	if (num_buckets <= 0) {
		throw InvalidInputException("Invalid bucket count: %d (must be > 0)", num_buckets);
	}

	int32_t hash_value = IcebergHash::HashValue(constant);
	// Iceberg spec: (hash & 0x7FFFFFFF) % num_buckets
	int32_t bucket_id = (hash_value & 0x7FFFFFFF) % num_buckets;

	return Value::INTEGER(bucket_id);
}

Value TruncateTransform::ApplyTransform(const Value &constant, const IcebergTransform &transform) {
	switch (constant.type().id()) {
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT: {
		auto v = constant.GetValue<int64_t>();
		auto W = transform.GetTruncateWidth();
		return Value::Numeric(constant.type(), v - (((v % W) + W) % W));
	}
	case LogicalTypeId::DECIMAL: {
		// Truncate the unscaled value, keeping the same scale and width
		auto scaled_constant = constant.Copy();
		scaled_constant.Reinterpret(LogicalType::BIGINT);
		auto v = scaled_constant.GetValue<int64_t>();
		auto W = transform.GetTruncateWidth();
		return Value::DECIMAL(int64_t(v - (((v % W) + W) % W)), DecimalType::GetWidth(constant.type()),
		                      DecimalType::GetScale(constant.type()));
	}
	case LogicalTypeId::BLOB: {
		// truncate to L bytes
		auto bytes = StringValue::Get(constant);
		return Value::BLOB((uint8_t *)bytes.data(),
		                   bytes.size() < transform.GetTruncateWidth() ? bytes.size() : transform.GetTruncateWidth());
	}
	case LogicalTypeId::VARCHAR: {
		// truncate to at most L code points, assuming UTF-8 encoding
		auto v = constant.GetValue<string>();
		auto L = transform.GetTruncateWidth();
		size_t num_characters = 0;
		for (auto cluster : Utf8Proc::GraphemeClusters(v.data(), v.size())) {
			if (++num_characters >= L) {
				return Value(v.substr(0, cluster.end));
			}
		}
		return constant;
	}
	default:
		throw NotImplementedException("'truncate' transform for type %s", constant.type().ToString());
	}
	return constant;
}
} // namespace duckdb
