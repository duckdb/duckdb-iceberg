
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/plan_status.hpp"

namespace duckdb {
namespace rest_api_objects {

class EmptyPlanningResult {
public:
	EmptyPlanningResult();
	EmptyPlanningResult(const EmptyPlanningResult &) = delete;
	EmptyPlanningResult &operator=(const EmptyPlanningResult &) = delete;
	EmptyPlanningResult(EmptyPlanningResult &&) = default;
	EmptyPlanningResult &operator=(EmptyPlanningResult &&) = default;

public:
	// Deserialization
	static EmptyPlanningResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	EmptyPlanningResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	PlanStatus status;
};

} // namespace rest_api_objects
} // namespace duckdb
