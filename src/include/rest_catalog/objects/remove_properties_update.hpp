
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemovePropertiesUpdate {
public:
	RemovePropertiesUpdate();
	RemovePropertiesUpdate(const RemovePropertiesUpdate &) = delete;
	RemovePropertiesUpdate &operator=(const RemovePropertiesUpdate &) = delete;
	RemovePropertiesUpdate(RemovePropertiesUpdate &&) = default;
	RemovePropertiesUpdate &operator=(RemovePropertiesUpdate &&) = default;

public:
	static RemovePropertiesUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	vector<string> removals;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
