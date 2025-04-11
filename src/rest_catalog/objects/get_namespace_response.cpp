
#include "rest_catalog/objects/get_namespace_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

GetNamespaceResponse::GetNamespaceResponse() {
}

GetNamespaceResponse GetNamespaceResponse::FromJSON(yyjson_val *obj) {
	GetNamespaceResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string GetNamespaceResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto _namespace_val = yyjson_obj_get(obj, "namespace");
	if (!_namespace_val) {
		return "GetNamespaceResponse required property 'namespace' is missing";
	} else {
		error = _namespace.TryFromJSON(_namespace_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		has_properties = true;
		properties = parse_object_of_strings(properties_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
