
#include "rest_catalog/objects/commit_view_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
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

string CommitViewRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto updates_val = yyjson_obj_get(obj, "updates");
	if (!updates_val) {
		return "CommitViewRequest required property 'updates' is missing";
	} else {
		if (yyjson_is_arr(updates_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(updates_val, idx, max, val) {
				ViewUpdate tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				updates.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("CommitViewRequest property 'updates' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(updates_val));
		}
	}
	auto identifier_val = yyjson_obj_get(obj, "identifier");
	if (identifier_val) {
		has_identifier = true;
		error = identifier.TryFromJSON(identifier_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto requirements_val = yyjson_obj_get(obj, "requirements");
	if (requirements_val) {
		has_requirements = true;
		if (yyjson_is_arr(requirements_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(requirements_val, idx, max, val) {
				ViewRequirement tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				requirements.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "CommitViewRequest property 'requirements' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(requirements_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
