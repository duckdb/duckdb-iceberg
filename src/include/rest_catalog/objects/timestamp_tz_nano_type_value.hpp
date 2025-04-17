
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TimestampTzNanoTypeValue {
public:
	TimestampTzNanoTypeValue();
	TimestampTzNanoTypeValue(const TimestampTzNanoTypeValue &) = delete;
	TimestampTzNanoTypeValue &operator=(const TimestampTzNanoTypeValue &) = delete;
	TimestampTzNanoTypeValue(TimestampTzNanoTypeValue &&) = default;
	TimestampTzNanoTypeValue &operator=(TimestampTzNanoTypeValue &&) = default;

public:
	static TimestampTzNanoTypeValue FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
