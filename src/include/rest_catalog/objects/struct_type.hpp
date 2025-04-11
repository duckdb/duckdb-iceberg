
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class StructField;

class StructType {
public:
	StructType();
	StructType(const StructType &) = delete;
	StructType &operator=(const StructType &) = delete;
	StructType(StructType &&) = default;
	StructType &operator=(StructType &&) = default;

public:
	static StructType FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string type;
	vector<unique_ptr<StructField>> fields;
};

} // namespace rest_api_objects
} // namespace duckdb
