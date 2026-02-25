#include "metadata/iceberg_transform.hpp"
#include "iceberg_hash.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

IcebergTransform::IcebergTransform() : raw_transform() {
	type = IcebergTransformType::INVALID;
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
		switch (input.id()) {
		case LogicalTypeId::DATE:
			return LogicalType::INTEGER;
		case LogicalTypeId::TIME:
			return LogicalType::BIGINT;
		case LogicalTypeId::TIMESTAMP:
			return LogicalType::BIGINT;
		case LogicalTypeId::TIMESTAMP_TZ:
			return LogicalType::BIGINT;
		case LogicalTypeId::TIMESTAMP_NS:
			return LogicalType::BIGINT;
		case LogicalTypeId::DECIMAL:
			return LogicalType::BLOB;
		case LogicalTypeId::UUID:
			return LogicalType::BLOB;
		default:
			return input;
		}
	}
	case IcebergTransformType::BUCKET:
		return LogicalType::INTEGER;
	case IcebergTransformType::TRUNCATE:
		return input;
	case IcebergTransformType::YEAR:
	case IcebergTransformType::MONTH:
	case IcebergTransformType::DAY:
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
	case IcebergTransformType::BUCKET:
		return LogicalType::INTEGER;
	case IcebergTransformType::TRUNCATE:
		return input;
	case IcebergTransformType::YEAR:
	case IcebergTransformType::MONTH:
	case IcebergTransformType::DAY:
	case IcebergTransformType::HOUR:
		return LogicalType::INTEGER;
	case IcebergTransformType::VOID:
		return input;
	default:
		throw InvalidConfigurationException("Can't produce a result type for transform %s and input type %s",
		                                    raw_transform, input.ToString());
	}
}

Value BucketTransform::ApplyTransform(const Value &constant, const IcebergTransform &transform) {
	if (constant.IsNull()) {
		// Iceberg spec: null values hash to 0, then apply modulo
		return Value::INTEGER(0);
	}

	// Get the number of buckets from the transform
	auto num_buckets = static_cast<int32_t>(transform.GetBucketModulo());

	// Safety check: avoid division by zero
	if (num_buckets <= 0) {
		throw InvalidInputException("Invalid bucket count: %d (must be > 0)", num_buckets);
	}

	// Compute the Iceberg-compatible hash
	int32_t hash_value = IcebergHash::HashValue(constant);

	// Apply modulo to get bucket number
	// Use & 0x7FFFFFFF to ensure positive result (Iceberg spec)
	int32_t bucket_id = (hash_value & 0x7FFFFFFF) % num_buckets;

	return Value::INTEGER(bucket_id);
}

} // namespace duckdb
