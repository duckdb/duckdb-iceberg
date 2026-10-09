
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class MaskToFixedValue {
public:
	MaskToFixedValue();
	MaskToFixedValue(const MaskToFixedValue &) = delete;
	MaskToFixedValue &operator=(const MaskToFixedValue &) = delete;
	MaskToFixedValue(MaskToFixedValue &&) = default;
	MaskToFixedValue &operator=(MaskToFixedValue &&) = default;

public:
	// Deserialization
	static MaskToFixedValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	MaskToFixedValue Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string action;
	int32_t field_id;
};

} // namespace rest_api_objects
} // namespace duckdb
