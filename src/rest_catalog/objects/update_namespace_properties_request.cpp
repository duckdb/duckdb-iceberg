
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
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(removals_val, idx, max, val) {
			auto tmp = yyjson_get_str(val);
			removals.emplace_back(std::move(tmp));
		}
	}
	auto updates_val = yyjson_obj_get(obj, "updates");
	if (updates_val) {
		has_updates = true;
		updates = parse_object_of_strings(updates_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
