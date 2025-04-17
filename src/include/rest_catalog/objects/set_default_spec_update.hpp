
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

class SetDefaultSpecUpdate {
public:
	SetDefaultSpecUpdate();
	SetDefaultSpecUpdate(const SetDefaultSpecUpdate &) = delete;
	SetDefaultSpecUpdate &operator=(const SetDefaultSpecUpdate &) = delete;
	SetDefaultSpecUpdate(SetDefaultSpecUpdate &&) = default;
	SetDefaultSpecUpdate &operator=(SetDefaultSpecUpdate &&) = default;

public:
	static SetDefaultSpecUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	int64_t spec_id;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
