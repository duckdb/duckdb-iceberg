
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class BooleanExpression {
public:
	BooleanExpression();
	BooleanExpression(const BooleanExpression &) = delete;
	BooleanExpression &operator=(const BooleanExpression &) = delete;
	BooleanExpression(BooleanExpression &&) = default;
	BooleanExpression &operator=(BooleanExpression &&) = default;

public:
	// Deserialization
	static BooleanExpression FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	BooleanExpression Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	bool value;
};

} // namespace rest_api_objects
} // namespace duckdb
