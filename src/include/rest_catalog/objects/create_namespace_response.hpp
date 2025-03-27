#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/namespace.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CreateNamespaceResponse {
public:
	static CreateNamespaceResponse FromJSON(yyjson_val *obj) {
		CreateNamespaceResponse result;
		auto _namespace_val = yyjson_obj_get(obj, "namespace");
		if (_namespace_val) {
			result._namespace = Namespace::FromJSON(_namespace_val);
		}
		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = parse_object_of_strings(properties_val);
		}
		return result;
	}
public:
	Namespace _namespace;
	ObjectOfStrings properties;
};

} // namespace rest_api_objects
} // namespace duckdb