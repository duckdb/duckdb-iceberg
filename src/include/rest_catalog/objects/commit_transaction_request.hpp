
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/commit_table_request.hpp"

using namespace duckdb_yyjson;

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
	static CommitTransactionRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<CommitTableRequest> table_changes;
};

} // namespace rest_api_objects
} // namespace duckdb
