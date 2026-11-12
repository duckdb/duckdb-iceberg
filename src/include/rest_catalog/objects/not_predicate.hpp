
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

class NotPredicate {
public:
	NotPredicate();
	NotPredicate(const NotPredicate &) = delete;
	NotPredicate &operator=(const NotPredicate &) = delete;
	NotPredicate(NotPredicate &&) = default;
	NotPredicate &operator=(NotPredicate &&) = default;

public:
	// Deserialization
	static NotPredicate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	NotPredicate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ExpressionType type;
	unique_ptr<Predicate> child;
};

} // namespace rest_api_objects
} // namespace duckdb
