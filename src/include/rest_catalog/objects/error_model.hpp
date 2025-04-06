
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ErrorModel {
public:
	ErrorModel();

public:
	static ErrorModel FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string message;
	string type;
	int64_t code;
	vector<string> stack;
};

} // namespace rest_api_objects
} // namespace duckdb
