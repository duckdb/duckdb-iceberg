
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/namespace.hpp"

namespace duckdb {
namespace rest_api_objects {

class CreateNamespaceResponse {
public:
	CreateNamespaceResponse();
	CreateNamespaceResponse(const CreateNamespaceResponse &) = delete;
	CreateNamespaceResponse &operator=(const CreateNamespaceResponse &) = delete;
	CreateNamespaceResponse(CreateNamespaceResponse &&) = default;
	CreateNamespaceResponse &operator=(CreateNamespaceResponse &&) = default;

public:
	// Deserialization
	static CreateNamespaceResponse FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CreateNamespaceResponse Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	Namespace _namespace;
	optional<unordered_map<string, string>> properties;
};

} // namespace rest_api_objects
} // namespace duckdb
