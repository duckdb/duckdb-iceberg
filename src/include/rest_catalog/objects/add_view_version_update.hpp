
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/view_version.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AddViewVersionUpdate {
public:
	AddViewVersionUpdate();
	AddViewVersionUpdate(const AddViewVersionUpdate &) = delete;
	AddViewVersionUpdate &operator=(const AddViewVersionUpdate &) = delete;
	AddViewVersionUpdate(AddViewVersionUpdate &&) = default;
	AddViewVersionUpdate &operator=(AddViewVersionUpdate &&) = default;

public:
	static AddViewVersionUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	ViewVersion view_version;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
