
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/catalog_object_identifier.hpp"
#include "rest_catalog/objects/page_token.hpp"

using namespace duckdb_yyjson;

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
	static ListFunctionsResponse FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	ListFunctionsResponse Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	optional<PageToken> next_page_token;
	optional<vector<CatalogObjectIdentifier>> identifiers;
};

} // namespace rest_api_objects
} // namespace duckdb
