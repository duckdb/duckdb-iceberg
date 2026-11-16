
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class NullTypeValue {
public:
	NullTypeValue();
	NullTypeValue(const NullTypeValue &) = delete;
	NullTypeValue &operator=(const NullTypeValue &) = delete;
	NullTypeValue(NullTypeValue &&) = default;
	NullTypeValue &operator=(NullTypeValue &&) = default;

public:
	// Deserialization
	static NullTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	NullTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	void *value;
};

} // namespace rest_api_objects
} // namespace duckdb
