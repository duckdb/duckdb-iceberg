
#include "rest_catalog/objects/remove_properties_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemovePropertiesUpdate::RemovePropertiesUpdate() {
}

RemovePropertiesUpdate RemovePropertiesUpdate::FromJSON(yyjson_val *obj) {
	RemovePropertiesUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RemovePropertiesUpdate RemovePropertiesUpdate::Copy() const {
	RemovePropertiesUpdate res;
	res.base_update = base_update.Copy();
	res.removals.reserve(removals.size());
	for (auto &item : removals) {
		res.removals.emplace_back(item);
	}
	return res;
}

string RemovePropertiesUpdate::TryFromJSON(yyjson_val *obj) {
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
			    "RemovePropertiesUpdate property 'action_refinement' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_refinement_val));
		}
		if (!yyjson_is_null(action_refinement_val) && action_refinement != "remove-properties") {
			return "RemovePropertiesUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "RemovePropertiesUpdate required property 'action' is missing";
	}
	auto removals_val = yyjson_obj_get(obj, "removals");
	if (!removals_val) {
		return "RemovePropertiesUpdate required property 'removals' is missing";
	} else {
		if (yyjson_is_arr(removals_val)) {
			size_t removals_idx, removals_max;
			yyjson_val *removals_item_val;
			yyjson_arr_foreach(removals_val, removals_idx, removals_max, removals_item_val) {
				string removals_item;
				if (yyjson_is_str(removals_item_val)) {
					removals_item = yyjson_get_str(removals_item_val);
				} else {
					return StringUtil::Format(
					    "RemovePropertiesUpdate property 'removals_item' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(removals_item_val));
				}
				removals.emplace_back(std::move(removals_item));
			}
		} else {
			return StringUtil::Format(
			    "RemovePropertiesUpdate property 'removals' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(removals_val));
		}
	}
	return "";
}

void RemovePropertiesUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: removals
	yyjson_mut_val *removals_arr = yyjson_mut_arr(doc);
	for (const auto &item : removals) {
		yyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());
		yyjson_mut_arr_append(removals_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "removals", removals_arr);
}

yyjson_mut_val *RemovePropertiesUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
