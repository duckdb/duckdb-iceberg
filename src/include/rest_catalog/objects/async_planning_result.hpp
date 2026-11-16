
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/plan_status.hpp"

namespace duckdb {
namespace rest_api_objects {

class AsyncPlanningResult {
public:
	AsyncPlanningResult();
	AsyncPlanningResult(const AsyncPlanningResult &) = delete;
	AsyncPlanningResult &operator=(const AsyncPlanningResult &) = delete;
	AsyncPlanningResult(AsyncPlanningResult &&) = default;
	AsyncPlanningResult &operator=(AsyncPlanningResult &&) = default;

public:
	// Deserialization
	static AsyncPlanningResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AsyncPlanningResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	PlanStatus status;
	string plan_id;
};

} // namespace rest_api_objects
} // namespace duckdb
