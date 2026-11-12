
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/action.hpp"

namespace duckdb {
namespace rest_api_objects {

class Predicate;

class ReadRestrictions {
public:
	ReadRestrictions();
	ReadRestrictions(const ReadRestrictions &) = delete;
	ReadRestrictions &operator=(const ReadRestrictions &) = delete;
	ReadRestrictions(ReadRestrictions &&) = default;
	ReadRestrictions &operator=(ReadRestrictions &&) = default;

public:
	// Deserialization
	static ReadRestrictions FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ReadRestrictions Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<vector<Action>> required_column_projections;
	unique_ptr<Predicate> required_row_filter;
};

} // namespace rest_api_objects
} // namespace duckdb
