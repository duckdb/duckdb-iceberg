
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TimestampTzTypeValue {
public:
	TimestampTzTypeValue();
	TimestampTzTypeValue(const TimestampTzTypeValue &) = delete;
	TimestampTzTypeValue &operator=(const TimestampTzTypeValue &) = delete;
	TimestampTzTypeValue(TimestampTzTypeValue &&) = default;
	TimestampTzTypeValue &operator=(TimestampTzTypeValue &&) = default;

public:
	static TimestampTzTypeValue FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
