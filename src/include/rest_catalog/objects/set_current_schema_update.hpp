
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class SetCurrentSchemaUpdate {
public:
	SetCurrentSchemaUpdate();
	SetCurrentSchemaUpdate(const SetCurrentSchemaUpdate &) = delete;
	SetCurrentSchemaUpdate &operator=(const SetCurrentSchemaUpdate &) = delete;
	SetCurrentSchemaUpdate(SetCurrentSchemaUpdate &&) = default;
	SetCurrentSchemaUpdate &operator=(SetCurrentSchemaUpdate &&) = default;

public:
	// Deserialization
	static SetCurrentSchemaUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SetCurrentSchemaUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	int32_t schema_id;
};

} // namespace rest_api_objects
} // namespace duckdb
