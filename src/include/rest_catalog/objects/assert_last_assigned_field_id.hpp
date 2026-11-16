
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class AssertLastAssignedFieldId {
public:
	AssertLastAssignedFieldId();
	AssertLastAssignedFieldId(const AssertLastAssignedFieldId &) = delete;
	AssertLastAssignedFieldId &operator=(const AssertLastAssignedFieldId &) = delete;
	AssertLastAssignedFieldId(AssertLastAssignedFieldId &&) = default;
	AssertLastAssignedFieldId &operator=(AssertLastAssignedFieldId &&) = default;

public:
	// Deserialization
	static AssertLastAssignedFieldId FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AssertLastAssignedFieldId Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	int32_t last_assigned_field_id;
};

} // namespace rest_api_objects
} // namespace duckdb
