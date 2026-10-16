
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class IdReference {
public:
	IdReference();
	IdReference(const IdReference &) = delete;
	IdReference &operator=(const IdReference &) = delete;
	IdReference(IdReference &&) = default;
	IdReference &operator=(IdReference &&) = default;

public:
	// Deserialization
	static IdReference FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	IdReference Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	int32_t id;
};

} // namespace rest_api_objects
} // namespace duckdb
