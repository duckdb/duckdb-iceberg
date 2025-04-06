
#include "rest_catalog/objects/commit_table_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
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

string CommitTableRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto requirements_val = yyjson_obj_get(obj, "requirements");
	if (!requirements_val) {
		return "CommitTableRequest required property 'requirements' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(requirements_val, idx, max, val) {
			TableRequirement tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			requirements.emplace_back(std::move(tmp));
		}
	}
	auto updates_val = yyjson_obj_get(obj, "updates");
	if (!updates_val) {
		return "CommitTableRequest required property 'updates' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(updates_val, idx, max, val) {
			TableUpdate tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			updates.emplace_back(std::move(tmp));
		}
	}
	auto identifier_val = yyjson_obj_get(obj, "identifier");
	if (identifier_val) {
		error = identifier.TryFromJSON(identifier_val);
		if (!error.empty()) {
			return error;
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
