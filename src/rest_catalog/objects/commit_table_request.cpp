
#include "rest_catalog/objects/commit_table_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CommitTableRequest::CommitTableRequest() {
}

CommitTableRequest CommitTableRequest::FromJSON(yyjson_val *obj) {
	CommitTableRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CommitTableRequest CommitTableRequest::Copy() const {
	CommitTableRequest res;
	res.requirements.reserve(requirements.size());
	for (auto &item : requirements) {
		res.requirements.emplace_back(item.Copy());
	}
	res.updates.reserve(updates.size());
	for (auto &item : updates) {
		res.updates.emplace_back(item.Copy());
	}
	if (identifier.has_value()) {
		res.identifier.emplace();
		(*res.identifier) = (*identifier).Copy();
	}
	return res;
}

string CommitTableRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto requirements_val = yyjson_obj_get(obj, "requirements");
	if (!requirements_val) {
		return "CommitTableRequest required property 'requirements' is missing";
	} else {
		if (yyjson_is_arr(requirements_val)) {
			size_t requirements_idx, requirements_max;
			yyjson_val *requirements_item_val;
			yyjson_arr_foreach(requirements_val, requirements_idx, requirements_max, requirements_item_val) {
				TableRequirement requirements_item;
				error = requirements_item.TryFromJSON(requirements_item_val);
				if (!error.empty()) {
					return error;
				}
				requirements.emplace_back(std::move(requirements_item));
			}
		} else {
			return StringUtil::Format(
			    "CommitTableRequest property 'requirements' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(requirements_val));
		}
	}
	auto updates_val = yyjson_obj_get(obj, "updates");
	if (!updates_val) {
		return "CommitTableRequest required property 'updates' is missing";
	} else {
		if (yyjson_is_arr(updates_val)) {
			size_t updates_idx, updates_max;
			yyjson_val *updates_item_val;
			yyjson_arr_foreach(updates_val, updates_idx, updates_max, updates_item_val) {
				TableUpdate updates_item;
				error = updates_item.TryFromJSON(updates_item_val);
				if (!error.empty()) {
					return error;
				}
				updates.emplace_back(std::move(updates_item));
			}
		} else {
			return StringUtil::Format(
			    "CommitTableRequest property 'updates' is not of type 'array', found '%s' instead",
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
	return "";
}

void CommitTableRequest::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: requirements
	yyjson_mut_val *requirements_arr = yyjson_mut_arr(doc);
	for (const auto &item : requirements) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(requirements_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "requirements", requirements_arr);

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
}

yyjson_mut_val *CommitTableRequest::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
