
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class MultiValuedMap {
public:
	MultiValuedMap();
	MultiValuedMap(const MultiValuedMap &) = delete;
	MultiValuedMap &operator=(const MultiValuedMap &) = delete;
	MultiValuedMap(MultiValuedMap &&) = default;
	MultiValuedMap &operator=(MultiValuedMap &&) = default;

public:
	// Deserialization
	static MultiValuedMap FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	MultiValuedMap Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	unordered_map<string, vector<string>> additional_properties;
};

} // namespace rest_api_objects
} // namespace duckdb
