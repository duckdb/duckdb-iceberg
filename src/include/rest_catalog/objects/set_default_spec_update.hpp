
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class SetDefaultSpecUpdate {
public:
	SetDefaultSpecUpdate();
	SetDefaultSpecUpdate(const SetDefaultSpecUpdate &) = delete;
	SetDefaultSpecUpdate &operator=(const SetDefaultSpecUpdate &) = delete;
	SetDefaultSpecUpdate(SetDefaultSpecUpdate &&) = default;
	SetDefaultSpecUpdate &operator=(SetDefaultSpecUpdate &&) = default;

public:
	// Deserialization
	static SetDefaultSpecUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SetDefaultSpecUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	int32_t spec_id;
};

} // namespace rest_api_objects
} // namespace duckdb
