
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class MaskAlphanum {
public:
	MaskAlphanum();
	MaskAlphanum(const MaskAlphanum &) = delete;
	MaskAlphanum &operator=(const MaskAlphanum &) = delete;
	MaskAlphanum(MaskAlphanum &&) = default;
	MaskAlphanum &operator=(MaskAlphanum &&) = default;

public:
	// Deserialization
	static MaskAlphanum FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	MaskAlphanum Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string action;
	int32_t field_id;
};

} // namespace rest_api_objects
} // namespace duckdb
