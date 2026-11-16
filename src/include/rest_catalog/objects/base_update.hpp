
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class BaseUpdate {
public:
	BaseUpdate();
	BaseUpdate(const BaseUpdate &) = delete;
	BaseUpdate &operator=(const BaseUpdate &) = delete;
	BaseUpdate(BaseUpdate &&) = default;
	BaseUpdate &operator=(BaseUpdate &&) = default;

public:
	// Deserialization
	static BaseUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	BaseUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string action;
};

} // namespace rest_api_objects
} // namespace duckdb
