
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class SetCurrentViewVersionUpdate {
public:
	SetCurrentViewVersionUpdate();
	SetCurrentViewVersionUpdate(const SetCurrentViewVersionUpdate &) = delete;
	SetCurrentViewVersionUpdate &operator=(const SetCurrentViewVersionUpdate &) = delete;
	SetCurrentViewVersionUpdate(SetCurrentViewVersionUpdate &&) = default;
	SetCurrentViewVersionUpdate &operator=(SetCurrentViewVersionUpdate &&) = default;

public:
	// Deserialization
	static SetCurrentViewVersionUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SetCurrentViewVersionUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	int32_t view_version_id;
};

} // namespace rest_api_objects
} // namespace duckdb
