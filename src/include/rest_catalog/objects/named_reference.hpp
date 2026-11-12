
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class NamedReference {
public:
	NamedReference();
	NamedReference(const NamedReference &) = delete;
	NamedReference &operator=(const NamedReference &) = delete;
	NamedReference(NamedReference &&) = default;
	NamedReference &operator=(NamedReference &&) = default;

public:
	// Deserialization
	static NamedReference FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	NamedReference Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	string name;
};

} // namespace rest_api_objects
} // namespace duckdb
