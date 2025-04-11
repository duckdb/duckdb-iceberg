
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/integer_type_value.hpp"
#include "rest_catalog/objects/long_type_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CountMap {
public:
	CountMap();
	CountMap(const CountMap &) = delete;
	CountMap &operator=(const CountMap &) = delete;
	CountMap(CountMap &&) = default;
	CountMap &operator=(CountMap &&) = default;

public:
	static CountMap FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<IntegerTypeValue> keys;
	bool has_keys = false;
	vector<LongTypeValue> values;
	bool has_values = false;
};

} // namespace rest_api_objects
} // namespace duckdb
