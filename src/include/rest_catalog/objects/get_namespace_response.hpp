
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/namespace.hpp"

namespace duckdb {
namespace rest_api_objects {

class GetNamespaceResponse {
public:
	GetNamespaceResponse();
	GetNamespaceResponse(const GetNamespaceResponse &) = delete;
	GetNamespaceResponse &operator=(const GetNamespaceResponse &) = delete;
	GetNamespaceResponse(GetNamespaceResponse &&) = default;
	GetNamespaceResponse &operator=(GetNamespaceResponse &&) = default;

public:
	// Deserialization
	static GetNamespaceResponse FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	GetNamespaceResponse Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	Namespace _namespace;
	optional<unordered_map<string, string>> properties;
};

} // namespace rest_api_objects
} // namespace duckdb
