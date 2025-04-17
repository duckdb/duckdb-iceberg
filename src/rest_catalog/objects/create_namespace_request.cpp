
#include "rest_catalog/objects/create_namespace_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CreateNamespaceRequest::CreateNamespaceRequest() {
}

CreateNamespaceRequest CreateNamespaceRequest::FromJSON(yyjson_val *obj) {
	CreateNamespaceRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string CreateNamespaceRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto _namespace_val = yyjson_obj_get(obj, "namespace");
	if (!_namespace_val) {
		return "CreateNamespaceRequest required property 'namespace' is missing";
	} else {
		error = _namespace.TryFromJSON(_namespace_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		has_properties = true;
		if (yyjson_is_obj(properties_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(properties_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "CreateNamespaceRequest property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				properties.emplace(key_str, std::move(tmp));
			}
		} else {
			return "CreateNamespaceRequest property 'properties' is not of type 'object'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
