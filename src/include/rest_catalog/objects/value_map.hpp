
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

public:
	static ValueMap FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<IntegerTypeValue> keys;
	vector<PrimitiveTypeValue> values;
};

} // namespace rest_api_objects
} // namespace duckdb
