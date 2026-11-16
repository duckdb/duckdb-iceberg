
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/statistics_file.hpp"

namespace duckdb {
namespace rest_api_objects {

class SetStatisticsUpdate {
public:
	SetStatisticsUpdate();
	SetStatisticsUpdate(const SetStatisticsUpdate &) = delete;
	SetStatisticsUpdate &operator=(const SetStatisticsUpdate &) = delete;
	SetStatisticsUpdate(SetStatisticsUpdate &&) = default;
	SetStatisticsUpdate &operator=(SetStatisticsUpdate &&) = default;

public:
	// Deserialization
	static SetStatisticsUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SetStatisticsUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	StatisticsFile statistics;
	optional<int64_t> snapshot_id;
};

} // namespace rest_api_objects
} // namespace duckdb
