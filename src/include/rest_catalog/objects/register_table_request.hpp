
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class RegisterTableRequest {
public:
	RegisterTableRequest();
	RegisterTableRequest(const RegisterTableRequest &) = delete;
	RegisterTableRequest &operator=(const RegisterTableRequest &) = delete;
	RegisterTableRequest(RegisterTableRequest &&) = default;
	RegisterTableRequest &operator=(RegisterTableRequest &&) = default;

public:
	// Deserialization
	static RegisterTableRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RegisterTableRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string name;
	string metadata_location;
	optional<bool> overwrite;
};

} // namespace rest_api_objects
} // namespace duckdb
