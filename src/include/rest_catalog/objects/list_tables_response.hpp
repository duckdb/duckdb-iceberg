
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/page_token.hpp"
#include "rest_catalog/objects/table_identifier.hpp"

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
	// Deserialization
	static ListTablesResponse FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ListTablesResponse Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<PageToken> next_page_token;
	optional<vector<TableIdentifier>> identifiers;
};

} // namespace rest_api_objects
} // namespace duckdb
