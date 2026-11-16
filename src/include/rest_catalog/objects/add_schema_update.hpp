
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/schema.hpp"

namespace duckdb {
namespace rest_api_objects {

class AddSchemaUpdate {
public:
	AddSchemaUpdate();
	AddSchemaUpdate(const AddSchemaUpdate &) = delete;
	AddSchemaUpdate &operator=(const AddSchemaUpdate &) = delete;
	AddSchemaUpdate(AddSchemaUpdate &&) = default;
	AddSchemaUpdate &operator=(AddSchemaUpdate &&) = default;

public:
	// Deserialization
	static AddSchemaUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AddSchemaUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	Schema schema;
	optional<int32_t> last_column_id;
};

} // namespace rest_api_objects
} // namespace duckdb
