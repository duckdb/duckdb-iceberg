
#include "rest_catalog/objects/sqlview_representation.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SQLViewRepresentation::SQLViewRepresentation() {
}

SQLViewRepresentation SQLViewRepresentation::FromJSON(yyjson_val *obj) {
	SQLViewRepresentation res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SQLViewRepresentation::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "SQLViewRepresentation required property 'type' is missing";
	} else {
		type = yyjson_get_str(type_val);
	}
	auto sql_val = yyjson_obj_get(obj, "sql");
	if (!sql_val) {
		return "SQLViewRepresentation required property 'sql' is missing";
	} else {
		sql = yyjson_get_str(sql_val);
	}
	auto dialect_val = yyjson_obj_get(obj, "dialect");
	if (!dialect_val) {
		return "SQLViewRepresentation required property 'dialect' is missing";
	} else {
		dialect = yyjson_get_str(dialect_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
