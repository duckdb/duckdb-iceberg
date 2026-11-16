
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class UpdateNamespacePropertiesResponse {
public:
	UpdateNamespacePropertiesResponse();
	UpdateNamespacePropertiesResponse(const UpdateNamespacePropertiesResponse &) = delete;
	UpdateNamespacePropertiesResponse &operator=(const UpdateNamespacePropertiesResponse &) = delete;
	UpdateNamespacePropertiesResponse(UpdateNamespacePropertiesResponse &&) = default;
	UpdateNamespacePropertiesResponse &operator=(UpdateNamespacePropertiesResponse &&) = default;

public:
	// Deserialization
	static UpdateNamespacePropertiesResponse FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	UpdateNamespacePropertiesResponse Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	vector<string> updated;
	vector<string> removed;
	optional<vector<string>> missing;
};

} // namespace rest_api_objects
} // namespace duckdb
