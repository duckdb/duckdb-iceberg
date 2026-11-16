
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/table_identifier.hpp"

namespace duckdb {
namespace rest_api_objects {

class RenameTableRequest {
public:
	RenameTableRequest();
	RenameTableRequest(const RenameTableRequest &) = delete;
	RenameTableRequest &operator=(const RenameTableRequest &) = delete;
	RenameTableRequest(RenameTableRequest &&) = default;
	RenameTableRequest &operator=(RenameTableRequest &&) = default;

public:
	// Deserialization
	static RenameTableRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RenameTableRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	TableIdentifier source;
	TableIdentifier destination;
};

} // namespace rest_api_objects
} // namespace duckdb
