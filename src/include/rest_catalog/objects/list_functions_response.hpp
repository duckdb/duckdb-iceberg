
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/catalog_object_identifier.hpp"
#include "rest_catalog/objects/page_token.hpp"

namespace duckdb {
namespace rest_api_objects {

class ListFunctionsResponse {
public:
	ListFunctionsResponse();
	ListFunctionsResponse(const ListFunctionsResponse &) = delete;
	ListFunctionsResponse &operator=(const ListFunctionsResponse &) = delete;
	ListFunctionsResponse(ListFunctionsResponse &&) = default;
	ListFunctionsResponse &operator=(ListFunctionsResponse &&) = default;

public:
	// Deserialization
	static ListFunctionsResponse FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ListFunctionsResponse Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<PageToken> next_page_token;
	optional<vector<CatalogObjectIdentifier>> identifiers;
};

} // namespace rest_api_objects
} // namespace duckdb
