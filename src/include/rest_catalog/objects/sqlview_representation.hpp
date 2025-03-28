#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SQLViewRepresentation {
public:
	static SQLViewRepresentation FromJSON(yyjson_val *obj) {
		SQLViewRepresentation result;

		auto dialect_val = yyjson_obj_get(obj, "dialect");
		if (dialect_val) {
			result.dialect = yyjson_get_str(dialect_val);
		}
		else {
			throw IOException("SQLViewRepresentation required property 'dialect' is missing");
		}

		auto sql_val = yyjson_obj_get(obj, "sql");
		if (sql_val) {
			result.sql = yyjson_get_str(sql_val);
		}
		else {
			throw IOException("SQLViewRepresentation required property 'sql' is missing");
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		else {
			throw IOException("SQLViewRepresentation required property 'type' is missing");
		}

		return result;
	}

public:
	string dialect;
	string sql;
	string type;
};
} // namespace rest_api_objects
} // namespace duckdb