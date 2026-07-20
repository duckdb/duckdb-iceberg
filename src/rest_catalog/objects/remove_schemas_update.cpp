
#include "rest_catalog/objects/remove_schemas_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

RemoveSchemasUpdate RemoveSchemasUpdate::Copy() const {
	RemoveSchemasUpdate res;
	res.base_update = base_update.Copy();
	res.schema_ids.reserve(schema_ids.size());
	for (auto &item : schema_ids) {
		res.schema_ids.emplace_back(item);
	}
	return res;
}

string RemoveSchemasUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto action_refinement_val = yyjson_obj_get(obj, "action");
	if (action_refinement_val) {
		string action_refinement;
		if (yyjson_is_str(action_refinement_val)) {
			action_refinement = yyjson_get_str(action_refinement_val);
		} else {
			return StringUtil::Format(
			    "RemoveSchemasUpdate property 'action_refinement' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_refinement_val));
		}
		if (!yyjson_is_null(action_refinement_val) && action_refinement != "remove-schemas") {
			return "RemoveSchemasUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "RemoveSchemasUpdate required property 'action' is missing";
	}
	auto schema_ids_val = yyjson_obj_get(obj, "schema-ids");
	if (!schema_ids_val) {
		return "RemoveSchemasUpdate required property 'schema-ids' is missing";
	} else {
		if (yyjson_is_arr(schema_ids_val)) {
			size_t schema_ids_idx, schema_ids_max;
			yyjson_val *schema_ids_item_val;
			yyjson_arr_foreach(schema_ids_val, schema_ids_idx, schema_ids_max, schema_ids_item_val) {
				int32_t schema_ids_item;
				if (yyjson_is_int(schema_ids_item_val)) {
					schema_ids_item = yyjson_get_int(schema_ids_item_val);
				} else {
					return StringUtil::Format(
					    "RemoveSchemasUpdate property 'schema_ids_item' is not of type 'integer', found '%s' instead",
					    yyjson_get_type_desc(schema_ids_item_val));
				}
				schema_ids.emplace_back(std::move(schema_ids_item));
			}
		} else {
			return StringUtil::Format(
			    "RemoveSchemasUpdate property 'schema_ids' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(schema_ids_val));
		}
	}
	return "";
}

void RemoveSchemasUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: schema-ids
	yyjson_mut_val *schema_ids_arr = yyjson_mut_arr(doc);
	for (const auto &item : schema_ids) {
		yyjson_mut_val *item_val = yyjson_mut_int(doc, item);
		yyjson_mut_arr_append(schema_ids_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "schema-ids", schema_ids_arr);
}

yyjson_mut_val *RemoveSchemasUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
