
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/expression_type.hpp"
#include "rest_catalog/objects/literals.hpp"
#include "rest_catalog/objects/term.hpp"

namespace duckdb {
namespace rest_api_objects {

class ValueExpression;

class SetPredicate {
public:
	SetPredicate();
	SetPredicate(const SetPredicate &) = delete;
	SetPredicate &operator=(const SetPredicate &) = delete;
	SetPredicate(SetPredicate &&) = default;
	SetPredicate &operator=(SetPredicate &&) = default;

public:
	// Deserialization
	static SetPredicate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SetPredicate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ExpressionType type;
	Literals values;
	unique_ptr<ValueExpression> child;
	optional<Term> term;
};

} // namespace rest_api_objects
} // namespace duckdb
