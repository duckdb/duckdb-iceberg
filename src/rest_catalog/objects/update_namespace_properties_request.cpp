
#include "rest_catalog/objects/update_namespace_properties_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

UpdateNamespacePropertiesRequest::UpdateNamespacePropertiesRequest() {
}

UpdateNamespacePropertiesRequest UpdateNamespacePropertiesRequest::FromJSON(yyjson_val *obj) {
	UpdateNamespacePropertiesRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

UpdateNamespacePropertiesRequest UpdateNamespacePropertiesRequest::Copy() const {
	UpdateNamespacePropertiesRequest res;
	if (removals.has_value()) {
		res.removals.emplace();
		(*res.removals).reserve((*removals).size());
		for (auto &item : (*removals)) {
			(*res.removals).emplace_back(item);
		}
	}
	if (updates.has_value()) {
		res.updates.emplace();
		for (auto &entry : (*updates)) {
			(*res.updates).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string UpdateNamespacePropertiesRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto removals_val = yyjson_obj_get(obj, "removals");
	if (removals_val) {
		vector<string> removals_tmp;
		if (yyjson_is_arr(removals_val)) {
			size_t removals_tmp_idx, removals_tmp_max;
			yyjson_val *removals_tmp_item_val;
			yyjson_arr_foreach(removals_val, removals_tmp_idx, removals_tmp_max, removals_tmp_item_val) {
				string removals_tmp_item;
				if (yyjson_is_str(removals_tmp_item_val)) {
					removals_tmp_item = yyjson_get_str(removals_tmp_item_val);
				} else {
					return StringUtil::Format("UpdateNamespacePropertiesRequest property 'removals_tmp_item' is not of "
					                          "type 'string', found '%s' instead",
					                          yyjson_get_type_desc(removals_tmp_item_val));
				}
				removals_tmp.emplace_back(std::move(removals_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "UpdateNamespacePropertiesRequest property 'removals_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(removals_val));
		}
		removals = std::move(removals_tmp);
	}
	auto updates_val = yyjson_obj_get(obj, "updates");
	if (updates_val) {
		case_insensitive_map_t<string> updates_tmp;
		if (yyjson_is_obj(updates_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(updates_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "UpdateNamespacePropertiesRequest property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				updates_tmp.emplace(key_str, std::move(tmp));
			}
		} else {
			return "UpdateNamespacePropertiesRequest property 'updates_tmp' is not of type 'object'";
		}
		updates = std::move(updates_tmp);
	}
	return "";
}

void UpdateNamespacePropertiesRequest::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: removals
	if (removals.has_value()) {
		auto &removals_value = *removals;
		yyjson_mut_val *removals_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : removals_value) {
			yyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());
			yyjson_mut_arr_append(removals_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "removals", removals_value_arr);
	}

	// Serialize: updates
	if (updates.has_value()) {
		auto &updates_value = *updates;
		yyjson_mut_val *updates_value_obj = yyjson_mut_obj(doc);
		for (const auto &it : updates_value) {
			auto &key = it.first;
			auto &value = it.second;
			auto key_ptr = unsafe_yyjson_mut_strncpy(doc, key.c_str(), strlen(key.c_str()));
			yyjson_mut_obj_add_strcpy(doc, updates_value_obj, key_ptr, value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "updates", updates_value_obj);
	}
}

yyjson_mut_val *UpdateNamespacePropertiesRequest::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
