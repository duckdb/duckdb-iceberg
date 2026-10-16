
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/completed_planning_result.hpp"

namespace duckdb {
namespace rest_api_objects {

class CompletedPlanningWithIDResult {
public:
	CompletedPlanningWithIDResult();
	CompletedPlanningWithIDResult(const CompletedPlanningWithIDResult &) = delete;
	CompletedPlanningWithIDResult &operator=(const CompletedPlanningWithIDResult &) = delete;
	CompletedPlanningWithIDResult(CompletedPlanningWithIDResult &&) = default;
	CompletedPlanningWithIDResult &operator=(CompletedPlanningWithIDResult &&) = default;
	class Object11 {
	public:
		Object11();
		Object11(const Object11 &) = delete;
		Object11 &operator=(const Object11 &) = delete;
		Object11(Object11 &&) = default;
		Object11 &operator=(Object11 &&) = default;

	public:
		// Deserialization
		static Object11 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object11 Copy() const;

		// Serialization
		void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		string plan_id;
	};

public:
	// Deserialization
	static CompletedPlanningWithIDResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CompletedPlanningWithIDResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	CompletedPlanningResult completed_planning_result;
	Object11 object_11;
};

} // namespace rest_api_objects
} // namespace duckdb
