
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class BooleanTypeValue {
public:
	BooleanTypeValue();
	BooleanTypeValue(const BooleanTypeValue &) = delete;
	BooleanTypeValue &operator=(const BooleanTypeValue &) = delete;
	BooleanTypeValue(BooleanTypeValue &&) = default;
	BooleanTypeValue &operator=(BooleanTypeValue &&) = default;

public:
	static BooleanTypeValue FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	bool value;
};

} // namespace rest_api_objects
} // namespace duckdb
