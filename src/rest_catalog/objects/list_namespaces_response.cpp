
#include "rest_catalog/objects/list_namespaces_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ListNamespacesResponse::ListNamespacesResponse() {
}

ListNamespacesResponse ListNamespacesResponse::FromJSON(yyjson_val *obj) {
	ListNamespacesResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ListNamespacesResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto next_page_token_val = yyjson_obj_get(obj, "next-page-token");
	if (next_page_token_val) {
		has_next_page_token = true;
		error = next_page_token.TryFromJSON(next_page_token_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto namespaces_val = yyjson_obj_get(obj, "namespaces");
	if (namespaces_val) {
		has_namespaces = true;
		if (yyjson_is_arr(namespaces_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(namespaces_val, idx, max, val) {
				Namespace tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				namespaces.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "ListNamespacesResponse property 'namespaces' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(namespaces_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
