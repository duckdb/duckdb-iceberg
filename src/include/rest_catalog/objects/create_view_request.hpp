
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/view_version.hpp"

namespace duckdb {
namespace rest_api_objects {

class CreateViewRequest {
public:
	CreateViewRequest();
	CreateViewRequest(const CreateViewRequest &) = delete;
	CreateViewRequest &operator=(const CreateViewRequest &) = delete;
	CreateViewRequest(CreateViewRequest &&) = default;
	CreateViewRequest &operator=(CreateViewRequest &&) = default;

public:
	// Deserialization
	static CreateViewRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CreateViewRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string name;
	Schema schema;
	ViewVersion view_version;
	unordered_map<string, string> properties;
	optional<string> location;
};

} // namespace rest_api_objects
} // namespace duckdb
