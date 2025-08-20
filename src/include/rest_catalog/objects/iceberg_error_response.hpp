
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/error_model.hpp"

using namespace duckdb_yyjson;

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
	static IcebergErrorResponse FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	ErrorModel _error;
};

} // namespace rest_api_objects
} // namespace duckdb
