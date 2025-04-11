
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/page_token.hpp"
#include "rest_catalog/objects/table_identifier.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ListTablesResponse {
public:
	ListTablesResponse();
	ListTablesResponse(const ListTablesResponse &) = delete;
	ListTablesResponse &operator=(const ListTablesResponse &) = delete;
	ListTablesResponse(ListTablesResponse &&) = default;
	ListTablesResponse &operator=(ListTablesResponse &&) = default;

public:
	static ListTablesResponse FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	PageToken next_page_token;
	bool has_next_page_token = false;
	vector<TableIdentifier> identifiers;
	bool has_identifiers = false;
};

} // namespace rest_api_objects
} // namespace duckdb
