
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/commit_table_request.hpp"

namespace duckdb {
namespace rest_api_objects {

class CommitTransactionRequest {
public:
	CommitTransactionRequest();
	CommitTransactionRequest(const CommitTransactionRequest &) = delete;
	CommitTransactionRequest &operator=(const CommitTransactionRequest &) = delete;
	CommitTransactionRequest(CommitTransactionRequest &&) = default;
	CommitTransactionRequest &operator=(CommitTransactionRequest &&) = default;

public:
	// Deserialization
	static CommitTransactionRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CommitTransactionRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	vector<CommitTableRequest> table_changes;
};

} // namespace rest_api_objects
} // namespace duckdb
