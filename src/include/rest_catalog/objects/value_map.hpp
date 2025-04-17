
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/integer_type_value.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ValueMap {
public:
	ValueMap();
	ValueMap(const ValueMap &) = delete;
	ValueMap &operator=(const ValueMap &) = delete;
	ValueMap(ValueMap &&) = default;
	ValueMap &operator=(ValueMap &&) = default;

public:
	static ValueMap FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<IntegerTypeValue> keys;
	bool has_keys = false;
	vector<PrimitiveTypeValue> values;
	bool has_values = false;
};

} // namespace rest_api_objects
} // namespace duckdb
