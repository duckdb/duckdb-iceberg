#include "../include/metadata/iceberg_transform.hpp"
#include "metadata/iceberg_transform.hpp"
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
	} else if (StringUtil::StartsWith(transform, "truncate")) {
		type = IcebergTransformType::TRUNCATE;
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
		//! The avro reader handles these
		return input;
		// switch (input.id()) {
		// case LogicalTypeId::DATE:
		// 	return LogicalType::DATE;
		// case LogicalTypeId::TIME:
		// 	return LogicalType::BIGINT;
		// case LogicalTypeId::TIMESTAMP:
		// 	return LogicalType::BIGINT;
		// case LogicalTypeId::TIMESTAMP_TZ:
		// 	return LogicalType::BIGINT;
		// case LogicalTypeId::TIMESTAMP_NS:
		// 	return LogicalType::BIGINT;
		// case LogicalTypeId::DECIMAL:
		// 	return LogicalType::BLOB;
		// case LogicalTypeId::UUID:
		// 	return LogicalType::BLOB;
		// default:
		// 	return input;
		// }
	}
	case IcebergTransformType::BUCKET:
		return LogicalType::INTEGER;
	case IcebergTransformType::TRUNCATE:
		return input;
	case IcebergTransformType::DAY: {
		child_list_t<LogicalType> members;
		members.emplace_back("int", LogicalType::INTEGER);
		members.emplace_back("date", LogicalType::DATE);
		return LogicalType::UNION(members);
	}
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

void IcebergTransform::SetBucketOrModuloValue(idx_t value) {
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

} // namespace duckdb
