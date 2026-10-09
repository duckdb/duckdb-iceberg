
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class TermReference {
public:
	TermReference();
	TermReference(const TermReference &) = delete;
	TermReference &operator=(const TermReference &) = delete;
	TermReference(TermReference &&) = default;
	TermReference &operator=(TermReference &&) = default;

public:
	// Deserialization
	static TermReference FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TermReference Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
