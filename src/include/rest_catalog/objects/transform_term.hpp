
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/term_reference.hpp"
#include "rest_catalog/objects/transform.hpp"

namespace duckdb {
namespace rest_api_objects {

class TransformTerm {
public:
	TransformTerm();
	TransformTerm(const TransformTerm &) = delete;
	TransformTerm &operator=(const TransformTerm &) = delete;
	TransformTerm(TransformTerm &&) = default;
	TransformTerm &operator=(TransformTerm &&) = default;

public:
	// Deserialization
	static TransformTerm FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TransformTerm Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	Transform transform;
	TermReference term;
};

} // namespace rest_api_objects
} // namespace duckdb
