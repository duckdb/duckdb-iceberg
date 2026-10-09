
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/expression_type.hpp"

namespace duckdb {
namespace rest_api_objects {

class Predicate;

class AndOrPredicate {
public:
	AndOrPredicate();
	AndOrPredicate(const AndOrPredicate &) = delete;
	AndOrPredicate &operator=(const AndOrPredicate &) = delete;
	AndOrPredicate(AndOrPredicate &&) = default;
	AndOrPredicate &operator=(AndOrPredicate &&) = default;

public:
	// Deserialization
	static AndOrPredicate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AndOrPredicate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ExpressionType type;
	unique_ptr<Predicate> left;
	unique_ptr<Predicate> right;
};

} // namespace rest_api_objects
} // namespace duckdb
