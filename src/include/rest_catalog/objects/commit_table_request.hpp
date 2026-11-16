
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/table_identifier.hpp"
#include "rest_catalog/objects/table_requirement.hpp"
#include "rest_catalog/objects/table_update.hpp"

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
	// Deserialization
	static CommitTableRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CommitTableRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	vector<TableRequirement> requirements;
	vector<TableUpdate> updates;
	optional<TableIdentifier> identifier;
};

} // namespace rest_api_objects
} // namespace duckdb
