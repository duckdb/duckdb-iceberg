
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/struct_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Schema {
public:
	Schema();
	class Object1 {
	public:
		Object1();

	public:
		static Object1 FromJSON(yyjson_val *obj);

	public:
		string TryFromJSON(yyjson_val *obj);

	public:
		int64_t schema_id;
		bool has_schema_id = false;
		vector<int64_t> identifier_field_ids;
		bool has_identifier_field_ids = false;
	};

public:
	static Schema FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	StructType struct_type;
	Object1 object_1;
};

} // namespace rest_api_objects
} // namespace duckdb
