
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/iceberg_error_response.hpp"
#include "rest_catalog/objects/plan_status.hpp"

namespace duckdb {
namespace rest_api_objects {

class FailedPlanningResult {
public:
	FailedPlanningResult();
	FailedPlanningResult(const FailedPlanningResult &) = delete;
	FailedPlanningResult &operator=(const FailedPlanningResult &) = delete;
	FailedPlanningResult(FailedPlanningResult &&) = default;
	FailedPlanningResult &operator=(FailedPlanningResult &&) = default;
	class Object12 {
	public:
		Object12();
		Object12(const Object12 &) = delete;
		Object12 &operator=(const Object12 &) = delete;
		Object12(Object12 &&) = default;
		Object12 &operator=(Object12 &&) = default;

	public:
		// Deserialization
		static Object12 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object12 Copy() const;

		// Serialization
		void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		PlanStatus status;
	};

public:
	// Deserialization
	static FailedPlanningResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FailedPlanningResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	IcebergErrorResponse iceberg_error_response;
	Object12 object_12;
};

} // namespace rest_api_objects
} // namespace duckdb
