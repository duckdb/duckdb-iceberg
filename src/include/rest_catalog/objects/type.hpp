
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list_type.hpp"
#include "rest_catalog/objects/map_type.hpp"
#include "rest_catalog/objects/primitive_type.hpp"
#include "rest_catalog/objects/struct_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Type {
public:
	Type();
	Type(const Type &) = delete;
	Type &operator=(const Type &) = delete;
	Type(Type &&) = default;
	Type &operator=(Type &&) = default;

public:
	static Type FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	PrimitiveType primitive_type;
	bool has_primitive_type = false;
	StructType struct_type;
	bool has_struct_type = false;
	ListType list_type;
	bool has_list_type = false;
	MapType map_type;
	bool has_map_type = false;
};

} // namespace rest_api_objects
} // namespace duckdb
