
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class RegisterViewRequest {
public:
	RegisterViewRequest();
	RegisterViewRequest(const RegisterViewRequest &) = delete;
	RegisterViewRequest &operator=(const RegisterViewRequest &) = delete;
	RegisterViewRequest(RegisterViewRequest &&) = default;
	RegisterViewRequest &operator=(RegisterViewRequest &&) = default;

public:
	// Deserialization
	static RegisterViewRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RegisterViewRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string name;
	string metadata_location;
};

} // namespace rest_api_objects
} // namespace duckdb
