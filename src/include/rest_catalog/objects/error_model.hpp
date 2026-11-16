
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class ErrorModel {
public:
	ErrorModel();
	ErrorModel(const ErrorModel &) = delete;
	ErrorModel &operator=(const ErrorModel &) = delete;
	ErrorModel(ErrorModel &&) = default;
	ErrorModel &operator=(ErrorModel &&) = default;

public:
	// Deserialization
	static ErrorModel FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ErrorModel Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string message;
	string type;
	int32_t code;
	optional<vector<string>> stack;
};

} // namespace rest_api_objects
} // namespace duckdb
