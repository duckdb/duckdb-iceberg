
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class ShowLast4 {
public:
	ShowLast4();
	ShowLast4(const ShowLast4 &) = delete;
	ShowLast4 &operator=(const ShowLast4 &) = delete;
	ShowLast4(ShowLast4 &&) = default;
	ShowLast4 &operator=(ShowLast4 &&) = default;

public:
	// Deserialization
	static ShowLast4 FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ShowLast4 Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string action;
	int32_t field_id;
};

} // namespace rest_api_objects
} // namespace duckdb
