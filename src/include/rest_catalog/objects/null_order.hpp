
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class NullOrder {
public:
	NullOrder();
	NullOrder(const NullOrder &) = delete;
	NullOrder &operator=(const NullOrder &) = delete;
	NullOrder(NullOrder &&) = default;
	NullOrder &operator=(NullOrder &&) = default;

public:
	// Deserialization
	static NullOrder FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	NullOrder Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
