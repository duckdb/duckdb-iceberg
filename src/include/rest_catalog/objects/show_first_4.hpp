
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class ShowFirst4 {
public:
	ShowFirst4();
	ShowFirst4(const ShowFirst4 &) = delete;
	ShowFirst4 &operator=(const ShowFirst4 &) = delete;
	ShowFirst4(ShowFirst4 &&) = default;
	ShowFirst4 &operator=(ShowFirst4 &&) = default;

public:
	// Deserialization
	static ShowFirst4 FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ShowFirst4 Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string action;
	int32_t field_id;
};

} // namespace rest_api_objects
} // namespace duckdb
