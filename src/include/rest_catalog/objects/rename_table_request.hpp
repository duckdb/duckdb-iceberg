
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_identifier.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RenameTableRequest {
public:
	RenameTableRequest();
	RenameTableRequest(const RenameTableRequest &) = delete;
	RenameTableRequest &operator=(const RenameTableRequest &) = delete;
	RenameTableRequest(RenameTableRequest &&) = default;
	RenameTableRequest &operator=(RenameTableRequest &&) = default;

public:
	static RenameTableRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	TableIdentifier source;
	TableIdentifier destination;
};

} // namespace rest_api_objects
} // namespace duckdb
