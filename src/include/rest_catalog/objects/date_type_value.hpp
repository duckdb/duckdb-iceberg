
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class DateTypeValue {
public:
	DateTypeValue();
	DateTypeValue(const DateTypeValue &) = delete;
	DateTypeValue &operator=(const DateTypeValue &) = delete;
	DateTypeValue(DateTypeValue &&) = default;
	DateTypeValue &operator=(DateTypeValue &&) = default;

public:
	// Deserialization
	static DateTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	DateTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
