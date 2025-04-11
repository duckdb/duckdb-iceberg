
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ExpressionType {
public:
	ExpressionType();
	ExpressionType(const ExpressionType &) = delete;
	ExpressionType &operator=(const ExpressionType &) = delete;
	ExpressionType(ExpressionType &&) = default;
	ExpressionType &operator=(ExpressionType &&) = default;

public:
	static ExpressionType FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
