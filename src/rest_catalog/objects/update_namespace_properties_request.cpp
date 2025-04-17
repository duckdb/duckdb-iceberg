
#include "rest_catalog/objects/update_namespace_properties_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
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

string UpdateNamespacePropertiesRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto removals_val = yyjson_obj_get(obj, "removals");
	if (removals_val) {
		has_removals = true;
		if (yyjson_is_arr(removals_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(removals_val, idx, max, val) {
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "UpdateNamespacePropertiesRequest property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				removals.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "UpdateNamespacePropertiesRequest property 'removals' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(removals_val));
		}
	}
	auto updates_val = yyjson_obj_get(obj, "updates");
	if (updates_val) {
		has_updates = true;
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
				updates.emplace(key_str, std::move(tmp));
			}
		} else {
			return "UpdateNamespacePropertiesRequest property 'updates' is not of type 'object'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
