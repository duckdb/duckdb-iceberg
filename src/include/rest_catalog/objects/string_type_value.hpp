
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class StringTypeValue {
public:
	StringTypeValue();
	StringTypeValue(const StringTypeValue &) = delete;
	StringTypeValue &operator=(const StringTypeValue &) = delete;
	StringTypeValue(StringTypeValue &&) = default;
	StringTypeValue &operator=(StringTypeValue &&) = default;

public:
	static StringTypeValue FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
