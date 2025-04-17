
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Type;

class StructField {
public:
	StructField();
	StructField(const StructField &) = delete;
	StructField &operator=(const StructField &) = delete;
	StructField(StructField &&) = default;
	StructField &operator=(StructField &&) = default;

public:
	static StructField FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int64_t id;
	string name;
	unique_ptr<Type> type;
	bool required;
	string doc;
	bool has_doc = false;
	PrimitiveTypeValue initial_default;
	bool has_initial_default = false;
	PrimitiveTypeValue write_default;
	bool has_write_default = false;
};

} // namespace rest_api_objects
} // namespace duckdb
