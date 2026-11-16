
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/partition_spec.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/sort_order.hpp"

namespace duckdb {
namespace rest_api_objects {

class CreateTableRequest {
public:
	CreateTableRequest();
	CreateTableRequest(const CreateTableRequest &) = delete;
	CreateTableRequest &operator=(const CreateTableRequest &) = delete;
	CreateTableRequest(CreateTableRequest &&) = default;
	CreateTableRequest &operator=(CreateTableRequest &&) = default;

public:
	// Deserialization
	static CreateTableRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CreateTableRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string name;
	Schema schema;
	optional<string> location;
	optional<PartitionSpec> partition_spec;
	optional<SortOrder> write_order;
	optional<bool> stage_create;
	optional<unordered_map<string, string>> properties;
};

} // namespace rest_api_objects
} // namespace duckdb
