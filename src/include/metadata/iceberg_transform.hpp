#pragma once

#include "metadata/iceberg_predicate_stats.hpp"

namespace duckdb {

enum class IcebergTransformType : uint8_t { IDENTITY, BUCKET, TRUNCATE, YEAR, MONTH, DAY, HOUR, VOID, INVALID };

struct IcebergTransform {
public:
	IcebergTransform();
	IcebergTransform(const string &transform);

public:
	operator IcebergTransformType() const {
		return Type();
	}

public:
	//! Singleton iceberg transform, used as stand-in when there is no partition field
	static const IcebergTransform &Identity() {
		static auto identity = IcebergTransform("identity");
		return identity;
	}
	idx_t GetBucketModulo() const {
		D_ASSERT(type == IcebergTransformType::BUCKET);
		return modulo;
	}
	idx_t GetTruncateWidth() const {
		D_ASSERT(type == IcebergTransformType::TRUNCATE);
		return width;
	}
	IcebergTransformType Type() const {
		D_ASSERT(type != IcebergTransformType::INVALID);
		return type;
	}
	const string &RawType() const {
		return raw_transform;
	}

	LogicalType GetSerializedType(const LogicalType &input) const;
	LogicalType GetBoundsType(const LogicalType &input) const;

private:
	//! Preserve the input for debugging
	string raw_transform;
	IcebergTransformType type;
	idx_t modulo;
	idx_t width;
};

struct IdentityTransform {
	static Value ApplyTransform(const Value &constant, const IcebergTransform &transform) {
		return constant;
	}
	static bool CompareEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return constant >= stats.lower_bound && constant <= stats.upper_bound;
	}
	static bool CompareLessThan(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.lower_bound < constant;
	}
	static bool CompareLessThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.lower_bound <= constant;
	}
	static bool CompareGreaterThan(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.upper_bound > constant;
	}
	static bool CompareGreaterThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.upper_bound >= constant;
	}
};

struct YearTransform {
	static Value ApplyTransform(const Value &constant, const IcebergTransform &transform) {
		switch (constant.type().id()) {
		case LogicalTypeId::TIMESTAMP: {
			auto val = constant.GetValue<timestamp_t>();
			auto components = Timestamp::GetComponents(val);
			return Value::INTEGER(components.year - 1970);
		}
		case LogicalTypeId::TIMESTAMP_TZ: {
			auto val = constant.GetValue<timestamp_tz_t>();
			auto components = Timestamp::GetComponents(val);
			return Value::INTEGER(components.year - 1970);
		}
		case LogicalTypeId::DATE: {
			int32_t year;
			int32_t month;
			int32_t day;
			Date::Convert(constant.GetValue<date_t>(), year, month, day);
			return Value::INTEGER(year - 1970);
		}
		default:
			throw NotImplementedException("'year' transform for type %s", constant.type().ToString());
		}
		return constant;
	}
	static bool CompareEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return constant >= stats.lower_bound && constant <= stats.upper_bound;
	}
	static bool CompareLessThan(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.lower_bound <= constant;
	}
	static bool CompareLessThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.lower_bound <= constant;
	}
	static bool CompareGreaterThan(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.upper_bound >= constant;
	}
	static bool CompareGreaterThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.upper_bound >= constant;
	}
};

struct MonthTransform {
	static Value ApplyTransform(const Value &constant, const IcebergTransform &transform) {
		switch (constant.type().id()) {
		case LogicalTypeId::TIMESTAMP: {
			auto val = constant.GetValue<timestamp_t>();
			auto diff = Interval::GetAge(val, timestamp_t::epoch());
			return Value::INTEGER(diff.months);
		}
		case LogicalTypeId::TIMESTAMP_TZ: {
			auto val = constant.GetValue<timestamp_tz_t>();
			auto diff = Interval::GetAge(val, timestamp_t::epoch());
			return Value::INTEGER(diff.months);
		}
		case LogicalTypeId::DATE: {
			int32_t year, month, day;
			Date::Convert(constant.GetValue<date_t>(), year, month, day);
			return Value::INTEGER((year - 1970) * 12 + (month - 1));
		}
		default:
			throw NotImplementedException("'month' transform for type %s", constant.type().ToString());
		}
		return constant;
	}
	static bool CompareEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return constant >= stats.lower_bound && constant <= stats.upper_bound;
	}
	static bool CompareLessThan(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.lower_bound <= constant;
	}
	static bool CompareLessThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.lower_bound <= constant;
	}
	static bool CompareGreaterThan(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.upper_bound >= constant;
	}
	static bool CompareGreaterThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.upper_bound >= constant;
	}
};

struct DayTransform {
	static Value ApplyTransform(const Value &constant, const IcebergTransform &transform) {
		switch (constant.type().id()) {
		case LogicalTypeId::TIMESTAMP: {
			auto val = constant.GetValue<timestamp_t>();
			auto diff = Interval::GetDifference(val, timestamp_t::epoch());
			return Value::INTEGER(diff.days);
		}
		case LogicalTypeId::TIMESTAMP_TZ: {
			auto val = constant.GetValue<timestamp_tz_t>();
			auto diff = Interval::GetDifference(val, timestamp_t::epoch());
			return Value::INTEGER(diff.days);
		}
		case LogicalTypeId::DATE: {
			auto val = constant.GetValue<date_t>();
			return Value::INTEGER(val.days);
		}
		default:
			throw NotImplementedException("'day' transform for type %s", constant.type().ToString());
		}
		return constant;
	}
	static bool CompareEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return constant >= stats.lower_bound && constant <= stats.upper_bound;
	}
	static bool CompareLessThan(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.lower_bound <= constant;
	}
	static bool CompareLessThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.lower_bound <= constant;
	}
	static bool CompareGreaterThan(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.upper_bound >= constant;
	}
	static bool CompareGreaterThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.upper_bound >= constant;
	}
};

struct HourTransform {
	static Value ApplyTransform(const Value &constant, const IcebergTransform &transform) {
		switch (constant.type().id()) {
		case LogicalTypeId::TIMESTAMP: {
			auto val = constant.GetValue<timestamp_t>();
			return Value::INTEGER(static_cast<int32_t>(val.value / Interval::MICROS_PER_HOUR));
		}
		case LogicalTypeId::TIMESTAMP_TZ: {
			auto val = constant.GetValue<timestamp_tz_t>();
			return Value::INTEGER(static_cast<int32_t>(val.value / Interval::MICROS_PER_HOUR));
		}
		default:
			throw NotImplementedException("'hour' transform for type %s", constant.type().ToString());
		}
		return constant;
	}
	static bool CompareEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return constant >= stats.lower_bound && constant <= stats.upper_bound;
	}
	static bool CompareLessThan(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.lower_bound <= constant;
	}
	static bool CompareLessThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.lower_bound <= constant;
	}
	static bool CompareGreaterThan(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.upper_bound >= constant;
	}
	static bool CompareGreaterThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.upper_bound >= constant;
	}
};

struct BucketTransform {
	static Value ApplyTransform(const Value &constant, const IcebergTransform &transform);

	static bool CompareEqual(const Value &constant, const IcebergPredicateStats &stats) {
		// ApplyTransform returns null for null inputs (Iceberg spec) and for
		// unsupported types; in both cases skip filtering (conservative).
		if (constant.IsNull() || stats.lower_bound.IsNull() || stats.upper_bound.IsNull()) {
			return true;
		}

		int32_t bucket_id = constant.GetValue<int32_t>();
		int32_t lower_bucket = stats.lower_bound.GetValue<int32_t>();
		int32_t upper_bucket = stats.upper_bound.GetValue<int32_t>();
		return (bucket_id >= lower_bucket && bucket_id <= upper_bucket);
	}
	static bool CompareLessThan(const Value &constant, const IcebergPredicateStats &stats) {
		// Bucket transform doesn't preserve order, so we can't filter on <
		return true;
	}
	static bool CompareLessThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		// Bucket transform doesn't preserve order, so we can't filter on <=
		return true;
	}
	static bool CompareGreaterThan(const Value &constant, const IcebergPredicateStats &stats) {
		// Bucket transform doesn't preserve order, so we can't filter on >
		return true;
	}
	static bool CompareGreaterThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		// Bucket transform doesn't preserve order, so we can't filter on >=
		return true;
	}
};

} // namespace duckdb
