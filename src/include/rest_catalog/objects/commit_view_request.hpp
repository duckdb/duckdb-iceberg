
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_identifier.hpp"
#include "rest_catalog/objects/view_requirement.hpp"
#include "rest_catalog/objects/view_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CommitViewRequest {
public:
	CommitViewRequest();
	CommitViewRequest(const CommitViewRequest &) = delete;
	CommitViewRequest &operator=(const CommitViewRequest &) = delete;
	CommitViewRequest(CommitViewRequest &&) = default;
	CommitViewRequest &operator=(CommitViewRequest &&) = default;

public:
	static CommitViewRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<ViewUpdate> updates;
	TableIdentifier identifier;
	bool has_identifier = false;
	vector<ViewRequirement> requirements;
	bool has_requirements = false;
};

} // namespace rest_api_objects
} // namespace duckdb
