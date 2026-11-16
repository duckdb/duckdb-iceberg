
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class Type;

class ListType {
public:
	ListType();
	ListType(const ListType &) = delete;
	ListType &operator=(const ListType &) = delete;
	ListType(ListType &&) = default;
	ListType &operator=(ListType &&) = default;

public:
	// Deserialization
	static ListType FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ListType Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	int32_t element_id;
	unique_ptr<Type> element;
	bool element_required;
};

} // namespace rest_api_objects
} // namespace duckdb
