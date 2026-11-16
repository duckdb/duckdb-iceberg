
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class AssertViewUUID {
public:
	AssertViewUUID();
	AssertViewUUID(const AssertViewUUID &) = delete;
	AssertViewUUID &operator=(const AssertViewUUID &) = delete;
	AssertViewUUID(AssertViewUUID &&) = default;
	AssertViewUUID &operator=(AssertViewUUID &&) = default;

public:
	// Deserialization
	static AssertViewUUID FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AssertViewUUID Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	string uuid;
};

} // namespace rest_api_objects
} // namespace duckdb
