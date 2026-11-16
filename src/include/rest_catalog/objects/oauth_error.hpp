
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class OAuthError {
public:
	OAuthError();
	OAuthError(const OAuthError &) = delete;
	OAuthError &operator=(const OAuthError &) = delete;
	OAuthError(OAuthError &&) = default;
	OAuthError &operator=(OAuthError &&) = default;

public:
	// Deserialization
	static OAuthError FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	OAuthError Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string _error;
	optional<string> error_description;
	optional<string> error_uri;
};

} // namespace rest_api_objects
} // namespace duckdb
