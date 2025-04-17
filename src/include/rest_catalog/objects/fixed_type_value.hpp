
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FixedTypeValue {
public:
	FixedTypeValue();
	FixedTypeValue(const FixedTypeValue &) = delete;
	FixedTypeValue &operator=(const FixedTypeValue &) = delete;
	FixedTypeValue(FixedTypeValue &&) = default;
	FixedTypeValue &operator=(FixedTypeValue &&) = default;

public:
	static FixedTypeValue FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
