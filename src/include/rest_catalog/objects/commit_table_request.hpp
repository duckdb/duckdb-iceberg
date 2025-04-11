
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_identifier.hpp"
#include "rest_catalog/objects/table_requirement.hpp"
#include "rest_catalog/objects/table_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CommitTableRequest {
public:
	CommitTableRequest();
	CommitTableRequest(const CommitTableRequest &) = delete;
	CommitTableRequest &operator=(const CommitTableRequest &) = delete;
	CommitTableRequest(CommitTableRequest &&) = default;
	CommitTableRequest &operator=(CommitTableRequest &&) = default;

public:
	static CommitTableRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<TableRequirement> requirements;
	vector<TableUpdate> updates;
	TableIdentifier identifier;
	bool has_identifier = false;
};

} // namespace rest_api_objects
} // namespace duckdb
