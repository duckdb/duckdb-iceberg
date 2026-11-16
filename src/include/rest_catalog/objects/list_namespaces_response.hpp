
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/namespace.hpp"
#include "rest_catalog/objects/page_token.hpp"

namespace duckdb {
namespace rest_api_objects {

class ListNamespacesResponse {
public:
	ListNamespacesResponse();
	ListNamespacesResponse(const ListNamespacesResponse &) = delete;
	ListNamespacesResponse &operator=(const ListNamespacesResponse &) = delete;
	ListNamespacesResponse(ListNamespacesResponse &&) = default;
	ListNamespacesResponse &operator=(ListNamespacesResponse &&) = default;

public:
	// Deserialization
	static ListNamespacesResponse FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ListNamespacesResponse Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<PageToken> next_page_token;
	optional<vector<Namespace>> namespaces;
};

} // namespace rest_api_objects
} // namespace duckdb
