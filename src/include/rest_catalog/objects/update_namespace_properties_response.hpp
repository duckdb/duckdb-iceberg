
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class UpdateNamespacePropertiesResponse {
public:
	UpdateNamespacePropertiesResponse();
	UpdateNamespacePropertiesResponse(const UpdateNamespacePropertiesResponse &) = delete;
	UpdateNamespacePropertiesResponse &operator=(const UpdateNamespacePropertiesResponse &) = delete;
	UpdateNamespacePropertiesResponse(UpdateNamespacePropertiesResponse &&) = default;
	UpdateNamespacePropertiesResponse &operator=(UpdateNamespacePropertiesResponse &&) = default;

public:
	static UpdateNamespacePropertiesResponse FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<string> updated;
	vector<string> removed;
	vector<string> missing;
	bool has_missing = false;
};

} // namespace rest_api_objects
} // namespace duckdb
