
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class FieldLabels {
public:
	FieldLabels();
	FieldLabels(const FieldLabels &) = delete;
	FieldLabels &operator=(const FieldLabels &) = delete;
	FieldLabels(FieldLabels &&) = default;
	FieldLabels &operator=(FieldLabels &&) = default;

public:
	// Deserialization
	static FieldLabels FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FieldLabels Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int32_t field_id;
	case_insensitive_map_t<string> labels;
};

} // namespace rest_api_objects
} // namespace duckdb
