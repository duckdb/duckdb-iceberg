
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class FieldStatistics {
public:
	FieldStatistics();
	FieldStatistics(const FieldStatistics &) = delete;
	FieldStatistics &operator=(const FieldStatistics &) = delete;
	FieldStatistics(FieldStatistics &&) = default;
	FieldStatistics &operator=(FieldStatistics &&) = default;

public:
	// Deserialization
	static FieldStatistics FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FieldStatistics Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<int32_t> avg_value_size_in_bytes;
};

} // namespace rest_api_objects
} // namespace duckdb
