
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class TokenType {
public:
	TokenType();
	TokenType(const TokenType &) = delete;
	TokenType &operator=(const TokenType &) = delete;
	TokenType(TokenType &&) = default;
	TokenType &operator=(TokenType &&) = default;

public:
	// Deserialization
	static TokenType FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TokenType Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
