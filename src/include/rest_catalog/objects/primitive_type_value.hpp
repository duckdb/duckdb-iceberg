
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/binary_type_value.hpp"
#include "rest_catalog/objects/boolean_type_value.hpp"
#include "rest_catalog/objects/date_type_value.hpp"
#include "rest_catalog/objects/decimal_type_value.hpp"
#include "rest_catalog/objects/double_type_value.hpp"
#include "rest_catalog/objects/fixed_type_value.hpp"
#include "rest_catalog/objects/float_type_value.hpp"
#include "rest_catalog/objects/integer_type_value.hpp"
#include "rest_catalog/objects/long_type_value.hpp"
#include "rest_catalog/objects/null_type_value.hpp"
#include "rest_catalog/objects/string_type_value.hpp"
#include "rest_catalog/objects/time_type_value.hpp"
#include "rest_catalog/objects/timestamp_nano_type_value.hpp"
#include "rest_catalog/objects/timestamp_type_value.hpp"
#include "rest_catalog/objects/timestamp_tz_nano_type_value.hpp"
#include "rest_catalog/objects/timestamp_tz_type_value.hpp"
#include "rest_catalog/objects/uuidtype_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PrimitiveTypeValue {
public:
	PrimitiveTypeValue();
	PrimitiveTypeValue(const PrimitiveTypeValue &) = delete;
	PrimitiveTypeValue &operator=(const PrimitiveTypeValue &) = delete;
	PrimitiveTypeValue(PrimitiveTypeValue &&) = default;
	PrimitiveTypeValue &operator=(PrimitiveTypeValue &&) = default;

public:
	// Deserialization
	static PrimitiveTypeValue FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	PrimitiveTypeValue Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	optional<NullTypeValue> null_type_value;
	optional<BooleanTypeValue> boolean_type_value;
	optional<IntegerTypeValue> integer_type_value;
	optional<LongTypeValue> long_type_value;
	optional<FloatTypeValue> float_type_value;
	optional<DoubleTypeValue> double_type_value;
	optional<DecimalTypeValue> decimal_type_value;
	optional<StringTypeValue> string_type_value;
	optional<UUIDTypeValue> uuidtype_value;
	optional<DateTypeValue> date_type_value;
	optional<TimeTypeValue> time_type_value;
	optional<TimestampTypeValue> timestamp_type_value;
	optional<TimestampTzTypeValue> timestamp_tz_type_value;
	optional<TimestampNanoTypeValue> timestamp_nano_type_value;
	optional<TimestampTzNanoTypeValue> timestamp_tz_nano_type_value;
	optional<FixedTypeValue> fixed_type_value;
	optional<BinaryTypeValue> binary_type_value;
};

} // namespace rest_api_objects
} // namespace duckdb
