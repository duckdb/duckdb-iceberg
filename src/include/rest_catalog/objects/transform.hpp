
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class Transform {
public:
	Transform();
	Transform(const Transform &) = delete;
	Transform &operator=(const Transform &) = delete;
	Transform(Transform &&) = default;
	Transform &operator=(Transform &&) = default;

public:
	// Deserialization
	static Transform FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Transform Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
