
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_metadata.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CommitTableResponse {
public:
	CommitTableResponse();
	CommitTableResponse(const CommitTableResponse &) = delete;
	CommitTableResponse &operator=(const CommitTableResponse &) = delete;
	CommitTableResponse(CommitTableResponse &&) = default;
	CommitTableResponse &operator=(CommitTableResponse &&) = default;

public:
	static CommitTableResponse FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string metadata_location;
	TableMetadata metadata;
};

} // namespace rest_api_objects
} // namespace duckdb
