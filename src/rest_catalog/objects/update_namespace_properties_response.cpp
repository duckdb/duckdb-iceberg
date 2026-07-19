
#include "rest_catalog/objects/update_namespace_properties_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

UpdateNamespacePropertiesResponse::UpdateNamespacePropertiesResponse() {
}

UpdateNamespacePropertiesResponse UpdateNamespacePropertiesResponse::FromJSON(yyjson_val *obj) {
	UpdateNamespacePropertiesResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

UpdateNamespacePropertiesResponse UpdateNamespacePropertiesResponse::Copy() const {
	UpdateNamespacePropertiesResponse res;
	res.updated.reserve(updated.size());
	for (auto &item : updated) {
		res.updated.emplace_back(item);
	}
	res.removed.reserve(removed.size());
	for (auto &item : removed) {
		res.removed.emplace_back(item);
	}
	if (missing.has_value()) {
		res.missing.emplace();
		(*res.missing).reserve((*missing).size());
		for (auto &item : (*missing)) {
			(*res.missing).emplace_back(item);
		}
	}
	return res;
}

string UpdateNamespacePropertiesResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto updated_val = yyjson_obj_get(obj, "updated");
	if (!updated_val) {
		return "UpdateNamespacePropertiesResponse required property 'updated' is missing";
	} else {
		if (yyjson_is_arr(updated_val)) {
			size_t updated_idx, updated_max;
			yyjson_val *updated_item_val;
			yyjson_arr_foreach(updated_val, updated_idx, updated_max, updated_item_val) {
				string updated_item;
				if (yyjson_is_str(updated_item_val)) {
					updated_item = yyjson_get_str(updated_item_val);
				} else {
					return StringUtil::Format("UpdateNamespacePropertiesResponse property 'updated_item' is not of "
					                          "type 'string', found '%s' instead",
					                          yyjson_get_type_desc(updated_item_val));
				}
				updated.emplace_back(std::move(updated_item));
			}
		} else {
			return StringUtil::Format(
			    "UpdateNamespacePropertiesResponse property 'updated' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(updated_val));
		}
	}
	auto removed_val = yyjson_obj_get(obj, "removed");
	if (!removed_val) {
		return "UpdateNamespacePropertiesResponse required property 'removed' is missing";
	} else {
		if (yyjson_is_arr(removed_val)) {
			size_t removed_idx, removed_max;
			yyjson_val *removed_item_val;
			yyjson_arr_foreach(removed_val, removed_idx, removed_max, removed_item_val) {
				string removed_item;
				if (yyjson_is_str(removed_item_val)) {
					removed_item = yyjson_get_str(removed_item_val);
				} else {
					return StringUtil::Format("UpdateNamespacePropertiesResponse property 'removed_item' is not of "
					                          "type 'string', found '%s' instead",
					                          yyjson_get_type_desc(removed_item_val));
				}
				removed.emplace_back(std::move(removed_item));
			}
		} else {
			return StringUtil::Format(
			    "UpdateNamespacePropertiesResponse property 'removed' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(removed_val));
		}
	}
	auto missing_val = yyjson_obj_get(obj, "missing");
	if (missing_val) {
		if (yyjson_is_null(missing_val)) {
			//! do nothing, property is explicitly nullable
		} else {
			vector<string> missing_tmp;
			if (yyjson_is_arr(missing_val)) {
				size_t missing_tmp_idx, missing_tmp_max;
				yyjson_val *missing_tmp_item_val;
				yyjson_arr_foreach(missing_val, missing_tmp_idx, missing_tmp_max, missing_tmp_item_val) {
					string missing_tmp_item;
					if (yyjson_is_str(missing_tmp_item_val)) {
						missing_tmp_item = yyjson_get_str(missing_tmp_item_val);
					} else {
						return StringUtil::Format("UpdateNamespacePropertiesResponse property 'missing_tmp_item' is "
						                          "not of type 'string', found '%s' instead",
						                          yyjson_get_type_desc(missing_tmp_item_val));
					}
					missing_tmp.emplace_back(std::move(missing_tmp_item));
				}
			} else {
				return StringUtil::Format("UpdateNamespacePropertiesResponse property 'missing_tmp' is not of type "
				                          "'array', found '%s' instead",
				                          yyjson_get_type_desc(missing_val));
			}
			missing = std::move(missing_tmp);
		}
	}
	return "";
}

void UpdateNamespacePropertiesResponse::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: updated
	yyjson_mut_val *updated_arr = yyjson_mut_arr(doc);
	for (const auto &item : updated) {
		yyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());
		yyjson_mut_arr_append(updated_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "updated", updated_arr);

	// Serialize: removed
	yyjson_mut_val *removed_arr = yyjson_mut_arr(doc);
	for (const auto &item : removed) {
		yyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());
		yyjson_mut_arr_append(removed_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "removed", removed_arr);

	// Serialize: missing
	if (missing.has_value()) {
		auto &missing_value = *missing;
		yyjson_mut_val *missing_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : missing_value) {
			yyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());
			yyjson_mut_arr_append(missing_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "missing", missing_value_arr);
	}
}

yyjson_mut_val *UpdateNamespacePropertiesResponse::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
