
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class PlanTask {
public:
	PlanTask();
	PlanTask(const PlanTask &) = delete;
	PlanTask &operator=(const PlanTask &) = delete;
	PlanTask(PlanTask &&) = default;
	PlanTask &operator=(PlanTask &&) = default;

public:
	// Deserialization
	static PlanTask FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	PlanTask Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
