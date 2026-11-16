
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/async_planning_result.hpp"
#include "rest_catalog/objects/completed_planning_with_idresult.hpp"
#include "rest_catalog/objects/empty_planning_result.hpp"
#include "rest_catalog/objects/failed_planning_result.hpp"

namespace duckdb {
namespace rest_api_objects {

class PlanTableScanResult {
public:
	PlanTableScanResult();
	PlanTableScanResult(const PlanTableScanResult &) = delete;
	PlanTableScanResult &operator=(const PlanTableScanResult &) = delete;
	PlanTableScanResult(PlanTableScanResult &&) = default;
	PlanTableScanResult &operator=(PlanTableScanResult &&) = default;

public:
	// Deserialization
	static PlanTableScanResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	PlanTableScanResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<CompletedPlanningWithIDResult> completed_planning_with_idresult;
	optional<FailedPlanningResult> failed_planning_result;
	optional<AsyncPlanningResult> async_planning_result;
	optional<EmptyPlanningResult> empty_planning_result;
};

} // namespace rest_api_objects
} // namespace duckdb
