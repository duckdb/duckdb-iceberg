
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/plan_status.hpp"
#include "rest_catalog/objects/scan_tasks.hpp"
#include "rest_catalog/objects/storage_credential.hpp"

namespace duckdb {
namespace rest_api_objects {

class CompletedPlanningResult {
public:
	CompletedPlanningResult();
	CompletedPlanningResult(const CompletedPlanningResult &) = delete;
	CompletedPlanningResult &operator=(const CompletedPlanningResult &) = delete;
	CompletedPlanningResult(CompletedPlanningResult &&) = default;
	CompletedPlanningResult &operator=(CompletedPlanningResult &&) = default;
	class Object10 {
	public:
		Object10();
		Object10(const Object10 &) = delete;
		Object10 &operator=(const Object10 &) = delete;
		Object10(Object10 &&) = default;
		Object10 &operator=(Object10 &&) = default;

	public:
		// Deserialization
		static Object10 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object10 Copy() const;

		// Serialization
		void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		PlanStatus status;
		optional<vector<StorageCredential>> storage_credentials;
	};

public:
	// Deserialization
	static CompletedPlanningResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CompletedPlanningResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ScanTasks scan_tasks;
	Object10 object_10;
};

} // namespace rest_api_objects
} // namespace duckdb
