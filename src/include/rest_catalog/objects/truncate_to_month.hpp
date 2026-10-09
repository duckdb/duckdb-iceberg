
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class TruncateToMonth {
public:
	TruncateToMonth();
	TruncateToMonth(const TruncateToMonth &) = delete;
	TruncateToMonth &operator=(const TruncateToMonth &) = delete;
	TruncateToMonth(TruncateToMonth &&) = default;
	TruncateToMonth &operator=(TruncateToMonth &&) = default;

public:
	// Deserialization
	static TruncateToMonth FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TruncateToMonth Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string action;
	int32_t field_id;
};

} // namespace rest_api_objects
} // namespace duckdb
