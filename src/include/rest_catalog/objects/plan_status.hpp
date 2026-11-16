
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class PlanStatus {
public:
	PlanStatus();
	PlanStatus(const PlanStatus &) = delete;
	PlanStatus &operator=(const PlanStatus &) = delete;
	PlanStatus(PlanStatus &&) = default;
	PlanStatus &operator=(PlanStatus &&) = default;

public:
	// Deserialization
	static PlanStatus FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	PlanStatus Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
