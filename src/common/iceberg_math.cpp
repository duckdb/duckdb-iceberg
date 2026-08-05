#include "common/iceberg_math.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

namespace {

template <class T>
bool TryStepInteger(const Value &value, bool increment, Value &result) {
	auto raw = value.GetValue<T>();
	if (increment) {
		if (raw == NumericLimits<T>::Maximum()) {
			return false;
		}
		raw = static_cast<T>(raw + 1);
	} else {
		if (raw == NumericLimits<T>::Minimum()) {
			return false;
		}
		raw = static_cast<T>(raw - 1);
	}
	result = Value::CreateValue<T>(raw);
	return true;
}

template <class T>
bool TryStepTimestamp(const Value &value, bool increment, Value &result) {
	auto raw = value.GetValue<T>();
	if (!raw.IsFinite()) {
		return false;
	}
	if (increment ? raw.value == NumericLimits<int64_t>::Maximum() : raw.value == NumericLimits<int64_t>::Minimum()) {
		return false;
	}
	T stepped(raw.value + (increment ? 1 : -1));
	if (!stepped.IsFinite()) {
		return false;
	}
	result = Value::CreateValue<T>(stepped);
	return true;
}

bool TryStepDate(const Value &value, bool increment, Value &result) {
	auto raw = value.GetValue<date_t>();
	if (!Value::IsFinite(raw)) {
		return false;
	}
	auto days = static_cast<int64_t>(raw.days) + (increment ? 1 : -1);
	date_t stepped(static_cast<int32_t>(days));
	if (static_cast<int64_t>(stepped.days) != days || !Value::IsFinite(stepped)) {
		return false;
	}
	result = Value::DATE(stepped);
	return true;
}

bool TryStep(const Value &value, bool increment, Value &result) {
	if (value.IsNull()) {
		return false;
	}
	switch (value.type().id()) {
	case LogicalTypeId::TINYINT:
		return TryStepInteger<int8_t>(value, increment, result);
	case LogicalTypeId::SMALLINT:
		return TryStepInteger<int16_t>(value, increment, result);
	case LogicalTypeId::INTEGER:
		return TryStepInteger<int32_t>(value, increment, result);
	case LogicalTypeId::BIGINT:
		return TryStepInteger<int64_t>(value, increment, result);
	case LogicalTypeId::HUGEINT:
		return TryStepInteger<hugeint_t>(value, increment, result);
	case LogicalTypeId::UTINYINT:
		return TryStepInteger<uint8_t>(value, increment, result);
	case LogicalTypeId::USMALLINT:
		return TryStepInteger<uint16_t>(value, increment, result);
	case LogicalTypeId::UINTEGER:
		return TryStepInteger<uint32_t>(value, increment, result);
	case LogicalTypeId::UBIGINT:
		return TryStepInteger<uint64_t>(value, increment, result);
	case LogicalTypeId::UHUGEINT:
		return TryStepInteger<uhugeint_t>(value, increment, result);
	case LogicalTypeId::DATE:
		return TryStepDate(value, increment, result);
	case LogicalTypeId::TIMESTAMP:
		return TryStepTimestamp<timestamp_t>(value, increment, result);
	case LogicalTypeId::TIMESTAMP_TZ:
		return TryStepTimestamp<timestamp_tz_t>(value, increment, result);
	case LogicalTypeId::TIMESTAMP_NS:
		return TryStepTimestamp<timestamp_ns_t>(value, increment, result);
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return TryStepTimestamp<timestamp_tz_ns_t>(value, increment, result);
	default:
		return false;
	}
}

} // namespace

bool IcebergTryIncrement(const Value &value, Value &result) {
	return TryStep(value, true, result);
}

bool IcebergTryDecrement(const Value &value, Value &result) {
	return TryStep(value, false, result);
}

} // namespace duckdb
