
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TimeTypeValue {
public:
	TimeTypeValue();
	TimeTypeValue(const TimeTypeValue &) = delete;
	TimeTypeValue &operator=(const TimeTypeValue &) = delete;
	TimeTypeValue(TimeTypeValue &&) = default;
	TimeTypeValue &operator=(TimeTypeValue &&) = default;

public:
	static TimeTypeValue FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
