
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/completed_planning_result.hpp"
#include "rest_catalog/objects/empty_planning_result.hpp"
#include "rest_catalog/objects/failed_planning_result.hpp"

namespace duckdb {
namespace rest_api_objects {

class FetchPlanningResult {
public:
	FetchPlanningResult();
	FetchPlanningResult(const FetchPlanningResult &) = delete;
	FetchPlanningResult &operator=(const FetchPlanningResult &) = delete;
	FetchPlanningResult(FetchPlanningResult &&) = default;
	FetchPlanningResult &operator=(FetchPlanningResult &&) = default;

public:
	// Deserialization
	static FetchPlanningResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FetchPlanningResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<CompletedPlanningResult> completed_planning_result;
	optional<FailedPlanningResult> failed_planning_result;
	optional<EmptyPlanningResult> empty_planning_result;
};

} // namespace rest_api_objects
} // namespace duckdb
