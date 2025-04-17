
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

class MapType {
public:
	MapType();
	MapType(const MapType &) = delete;
	MapType &operator=(const MapType &) = delete;
	MapType(MapType &&) = default;
	MapType &operator=(MapType &&) = default;

public:
	static MapType FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string type;
	int64_t key_id;
	unique_ptr<Type> key;
	int64_t value_id;
	unique_ptr<Type> value;
	bool value_required;
};

} // namespace rest_api_objects
} // namespace duckdb
