
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class IntegerTypeValue {
public:
	IntegerTypeValue();
	IntegerTypeValue(const IntegerTypeValue &) = delete;
	IntegerTypeValue &operator=(const IntegerTypeValue &) = delete;
	IntegerTypeValue(IntegerTypeValue &&) = default;
	IntegerTypeValue &operator=(IntegerTypeValue &&) = default;

public:
	static IntegerTypeValue FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int32_t value;
};

} // namespace rest_api_objects
} // namespace duckdb
