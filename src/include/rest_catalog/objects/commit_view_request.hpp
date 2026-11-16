
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/table_identifier.hpp"
#include "rest_catalog/objects/view_requirement.hpp"
#include "rest_catalog/objects/view_update.hpp"

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
	// Deserialization
	static CommitViewRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CommitViewRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	vector<ViewUpdate> updates;
	optional<TableIdentifier> identifier;
	optional<vector<ViewRequirement>> requirements;
};

} // namespace rest_api_objects
} // namespace duckdb
