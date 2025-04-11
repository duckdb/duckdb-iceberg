
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Type;

class ListType {
public:
	ListType();
	ListType(const ListType &) = delete;
	ListType &operator=(const ListType &) = delete;
	ListType(ListType &&) = default;
	ListType &operator=(ListType &&) = default;

public:
	static ListType FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string type;
	int64_t element_id;
	unique_ptr<Type> element;
	bool element_required;
};

} // namespace rest_api_objects
} // namespace duckdb
