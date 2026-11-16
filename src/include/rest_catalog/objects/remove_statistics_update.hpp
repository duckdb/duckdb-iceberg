
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class RemoveStatisticsUpdate {
public:
	RemoveStatisticsUpdate();
	RemoveStatisticsUpdate(const RemoveStatisticsUpdate &) = delete;
	RemoveStatisticsUpdate &operator=(const RemoveStatisticsUpdate &) = delete;
	RemoveStatisticsUpdate(RemoveStatisticsUpdate &&) = default;
	RemoveStatisticsUpdate &operator=(RemoveStatisticsUpdate &&) = default;

public:
	// Deserialization
	static RemoveStatisticsUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RemoveStatisticsUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	int64_t snapshot_id;
};

} // namespace rest_api_objects
} // namespace duckdb
