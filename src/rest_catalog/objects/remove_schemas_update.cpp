
#include "rest_catalog/objects/remove_schemas_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemoveSchemasUpdate::RemoveSchemasUpdate() {
}

RemoveSchemasUpdate RemoveSchemasUpdate::FromJSON(yyjson_val *obj) {
	RemoveSchemasUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string RemoveSchemasUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto schema_ids_val = yyjson_obj_get(obj, "schema-ids");
	if (!schema_ids_val) {
		return "RemoveSchemasUpdate required property 'schema-ids' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(schema_ids_val, idx, max, val) {
			auto tmp = yyjson_get_sint(val);
			schema_ids.emplace_back(std::move(tmp));
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		action = yyjson_get_str(action_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
