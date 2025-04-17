
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class UpdateNamespacePropertiesRequest {
public:
	UpdateNamespacePropertiesRequest();
	UpdateNamespacePropertiesRequest(const UpdateNamespacePropertiesRequest &) = delete;
	UpdateNamespacePropertiesRequest &operator=(const UpdateNamespacePropertiesRequest &) = delete;
	UpdateNamespacePropertiesRequest(UpdateNamespacePropertiesRequest &&) = default;
	UpdateNamespacePropertiesRequest &operator=(UpdateNamespacePropertiesRequest &&) = default;

public:
	static UpdateNamespacePropertiesRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<string> removals;
	bool has_removals = false;
	case_insensitive_map_t<string> updates;
	bool has_updates = false;
};

} // namespace rest_api_objects
} // namespace duckdb
