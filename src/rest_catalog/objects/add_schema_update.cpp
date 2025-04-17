
#include "rest_catalog/objects/add_schema_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AddSchemaUpdate::AddSchemaUpdate() {
}

AddSchemaUpdate AddSchemaUpdate::FromJSON(yyjson_val *obj) {
	AddSchemaUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string AddSchemaUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto schema_val = yyjson_obj_get(obj, "schema");
	if (!schema_val) {
		return "AddSchemaUpdate required property 'schema' is missing";
	} else {
		error = schema.TryFromJSON(schema_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format("AddSchemaUpdate property 'action' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(action_val));
		}
	}
	auto last_column_id_val = yyjson_obj_get(obj, "last-column-id");
	if (last_column_id_val) {
		has_last_column_id = true;
		if (yyjson_is_int(last_column_id_val)) {
			last_column_id = yyjson_get_int(last_column_id_val);
		} else {
			return StringUtil::Format(
			    "AddSchemaUpdate property 'last_column_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(last_column_id_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
