
#include "rest_catalog/objects/set_current_schema_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetCurrentSchemaUpdate::SetCurrentSchemaUpdate() {
}

SetCurrentSchemaUpdate SetCurrentSchemaUpdate::FromJSON(yyjson_val *obj) {
	SetCurrentSchemaUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SetCurrentSchemaUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto schema_id_val = yyjson_obj_get(obj, "schema-id");
	if (!schema_id_val) {
		return "SetCurrentSchemaUpdate required property 'schema-id' is missing";
	} else {
		if (yyjson_is_int(schema_id_val)) {
			schema_id = yyjson_get_int(schema_id_val);
		} else {
			return StringUtil::Format(
			    "SetCurrentSchemaUpdate property 'schema_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(schema_id_val));
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format(
			    "SetCurrentSchemaUpdate property 'action' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
