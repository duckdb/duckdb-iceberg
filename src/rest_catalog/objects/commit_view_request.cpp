
#include "rest_catalog/objects/commit_view_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CommitViewRequest::CommitViewRequest() {
}

CommitViewRequest CommitViewRequest::FromJSON(yyjson_val *obj) {
	CommitViewRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CommitViewRequest CommitViewRequest::Copy() const {
	CommitViewRequest res;
	res.updates.reserve(updates.size());
	for (auto &item : updates) {
		res.updates.emplace_back(item.Copy());
	}
	if (identifier.has_value()) {
		res.identifier.emplace();
		(*res.identifier) = (*identifier).Copy();
	}
	if (requirements.has_value()) {
		res.requirements.emplace();
		(*res.requirements).reserve((*requirements).size());
		for (auto &item : (*requirements)) {
			(*res.requirements).emplace_back(item.Copy());
		}
	}
	return res;
}

string CommitViewRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto updates_val = yyjson_obj_get(obj, "updates");
	if (!updates_val) {
		return "CommitViewRequest required property 'updates' is missing";
	} else {
		if (yyjson_is_arr(updates_val)) {
			size_t updates_idx, updates_max;
			yyjson_val *updates_item_val;
			yyjson_arr_foreach(updates_val, updates_idx, updates_max, updates_item_val) {
				ViewUpdate updates_item;
				error = updates_item.TryFromJSON(updates_item_val);
				if (!error.empty()) {
					return error;
				}
				updates.emplace_back(std::move(updates_item));
			}
		} else {
			return StringUtil::Format("CommitViewRequest property 'updates' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(updates_val));
		}
	}
	auto identifier_val = yyjson_obj_get(obj, "identifier");
	if (identifier_val) {
		TableIdentifier identifier_tmp;
		error = identifier_tmp.TryFromJSON(identifier_val);
		if (!error.empty()) {
			return error;
		}
		identifier = std::move(identifier_tmp);
	}
	auto requirements_val = yyjson_obj_get(obj, "requirements");
	if (requirements_val) {
		vector<ViewRequirement> requirements_tmp;
		if (yyjson_is_arr(requirements_val)) {
			size_t requirements_tmp_idx, requirements_tmp_max;
			yyjson_val *requirements_tmp_item_val;
			yyjson_arr_foreach(requirements_val, requirements_tmp_idx, requirements_tmp_max,
			                   requirements_tmp_item_val) {
				ViewRequirement requirements_tmp_item;
				error = requirements_tmp_item.TryFromJSON(requirements_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				requirements_tmp.emplace_back(std::move(requirements_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "CommitViewRequest property 'requirements_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(requirements_val));
		}
		requirements = std::move(requirements_tmp);
	}
	return "";
}

void CommitViewRequest::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: updates
	yyjson_mut_val *updates_arr = yyjson_mut_arr(doc);
	for (const auto &item : updates) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(updates_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "updates", updates_arr);

	// Serialize: identifier
	if (identifier.has_value()) {
		auto &identifier_value = *identifier;
		yyjson_mut_val *identifier_value_val = identifier_value.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "identifier", identifier_value_val);
	}

	// Serialize: requirements
	if (requirements.has_value()) {
		auto &requirements_value = *requirements;
		yyjson_mut_val *requirements_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : requirements_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(requirements_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "requirements", requirements_value_arr);
	}
}

yyjson_mut_val *CommitViewRequest::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
