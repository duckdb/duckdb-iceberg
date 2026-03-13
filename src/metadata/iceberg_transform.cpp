#include "metadata/iceberg_transform.hpp"
#include "iceberg_hash.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"

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

string IcebergTransform::PartitionValueToString(const Value &partition_value) const {
	if (partition_value.IsNull() || partition_value.ToString() == "NULL") {
		return "NULL";
	}
	switch (type) {
	case IcebergTransformType::DAY: {
		date_t d;
		d.days = partition_value.GetValue<int32_t>();
		return Date::ToString(d);
	}
	case IcebergTransformType::MONTH: {
		int32_t m = partition_value.GetValue<int32_t>();
		// Floor-divide to correctly handle months before 1970
		int32_t year = 1970 + (m >= 0 ? m : m - 11) / 12;
		int32_t month = ((m % 12) + 12) % 12 + 1;
		return StringUtil::Format("%04d-%02d", year, month);
	}
	case IcebergTransformType::YEAR: {
		return std::to_string(1970 + partition_value.GetValue<int32_t>());
	}
	case IcebergTransformType::HOUR: {
		int64_t hours = partition_value.GetValue<int32_t>();
		timestamp_t ts(hours * Interval::MICROS_PER_HOUR);
		return Timestamp::ToString(ts);
	}
	default:
		return partition_value.ToString();
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

} // namespace duckdb
