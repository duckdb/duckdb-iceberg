
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

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
	static ErrorModel FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string message;
	string type;
	int32_t code;
	vector<string> stack;
	bool has_stack = false;
};

} // namespace rest_api_objects
} // namespace duckdb
