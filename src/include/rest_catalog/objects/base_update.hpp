
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class BaseUpdate {
public:
	BaseUpdate();
	BaseUpdate(const BaseUpdate &) = delete;
	BaseUpdate &operator=(const BaseUpdate &) = delete;
	BaseUpdate(BaseUpdate &&) = default;
	BaseUpdate &operator=(BaseUpdate &&) = default;

public:
	static BaseUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string action;
};

} // namespace rest_api_objects
} // namespace duckdb
