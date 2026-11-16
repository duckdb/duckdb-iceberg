
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class UpdateNamespacePropertiesRequest {
public:
	UpdateNamespacePropertiesRequest();
	UpdateNamespacePropertiesRequest(const UpdateNamespacePropertiesRequest &) = delete;
	UpdateNamespacePropertiesRequest &operator=(const UpdateNamespacePropertiesRequest &) = delete;
	UpdateNamespacePropertiesRequest(UpdateNamespacePropertiesRequest &&) = default;
	UpdateNamespacePropertiesRequest &operator=(UpdateNamespacePropertiesRequest &&) = default;

public:
	// Deserialization
	static UpdateNamespacePropertiesRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	UpdateNamespacePropertiesRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<vector<string>> removals;
	optional<unordered_map<string, string>> updates;
};

} // namespace rest_api_objects
} // namespace duckdb
