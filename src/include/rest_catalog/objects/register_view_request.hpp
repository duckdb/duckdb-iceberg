
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

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
	static RegisterViewRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string name;
	string metadata_location;
};

} // namespace rest_api_objects
} // namespace duckdb
