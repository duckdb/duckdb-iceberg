
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/assert_view_uuid.hpp"

namespace duckdb {
namespace rest_api_objects {

class ViewRequirement {
public:
	ViewRequirement();
	ViewRequirement(const ViewRequirement &) = delete;
	ViewRequirement &operator=(const ViewRequirement &) = delete;
	ViewRequirement(ViewRequirement &&) = default;
	ViewRequirement &operator=(ViewRequirement &&) = default;

public:
	// Deserialization
	static ViewRequirement FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ViewRequirement Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<AssertViewUUID> assert_view_uuid;
};

} // namespace rest_api_objects
} // namespace duckdb
