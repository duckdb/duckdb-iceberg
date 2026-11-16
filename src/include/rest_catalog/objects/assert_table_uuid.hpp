
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class AssertTableUUID {
public:
	AssertTableUUID();
	AssertTableUUID(const AssertTableUUID &) = delete;
	AssertTableUUID &operator=(const AssertTableUUID &) = delete;
	AssertTableUUID(AssertTableUUID &&) = default;
	AssertTableUUID &operator=(AssertTableUUID &&) = default;

public:
	// Deserialization
	static AssertTableUUID FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AssertTableUUID Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	string uuid;
};

} // namespace rest_api_objects
} // namespace duckdb
