
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/term_reference.hpp"
#include "rest_catalog/objects/transform_term.hpp"

namespace duckdb {
namespace rest_api_objects {

class Term {
public:
	Term();
	Term(const Term &) = delete;
	Term &operator=(const Term &) = delete;
	Term(Term &&) = default;
	Term &operator=(Term &&) = default;

public:
	// Deserialization
	static Term FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Term Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<TermReference> term_reference;
	optional<TransformTerm> transform_term;
};

} // namespace rest_api_objects
} // namespace duckdb
