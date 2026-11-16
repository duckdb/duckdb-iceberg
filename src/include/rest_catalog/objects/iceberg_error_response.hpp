
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/error_model.hpp"

namespace duckdb {
namespace rest_api_objects {

class IcebergErrorResponse {
public:
	IcebergErrorResponse();
	IcebergErrorResponse(const IcebergErrorResponse &) = delete;
	IcebergErrorResponse &operator=(const IcebergErrorResponse &) = delete;
	IcebergErrorResponse(IcebergErrorResponse &&) = default;
	IcebergErrorResponse &operator=(IcebergErrorResponse &&) = default;

public:
	// Deserialization
	static IcebergErrorResponse FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	IcebergErrorResponse Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ErrorModel _error;
};

} // namespace rest_api_objects
} // namespace duckdb
