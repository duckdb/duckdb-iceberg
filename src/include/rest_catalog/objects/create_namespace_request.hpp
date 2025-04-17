
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/namespace.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CreateNamespaceRequest {
public:
	CreateNamespaceRequest();
	CreateNamespaceRequest(const CreateNamespaceRequest &) = delete;
	CreateNamespaceRequest &operator=(const CreateNamespaceRequest &) = delete;
	CreateNamespaceRequest(CreateNamespaceRequest &&) = default;
	CreateNamespaceRequest &operator=(CreateNamespaceRequest &&) = default;

public:
	static CreateNamespaceRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	Namespace _namespace;
	case_insensitive_map_t<string> properties;
	bool has_properties = false;
};

} // namespace rest_api_objects
} // namespace duckdb
