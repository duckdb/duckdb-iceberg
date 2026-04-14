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
	return IcebergHash::BucketValue(constant, static_cast<int32_t>(transform.GetBucketModulo()));
}

Value TruncateTransform::ApplyTransform(const Value &constant, const IcebergTransform &transform) {
	if (constant.IsNull()) {
		// Iceberg spec: "All transforms must return null for a null input value"
		return Value(constant.type());
	}
	return IcebergHash::TruncateValue(constant, transform.GetTruncateWidth());
}
} // namespace duckdb
