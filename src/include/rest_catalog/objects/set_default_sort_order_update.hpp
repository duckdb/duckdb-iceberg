
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

class SetDefaultSortOrderUpdate {
public:
	SetDefaultSortOrderUpdate();
	SetDefaultSortOrderUpdate(const SetDefaultSortOrderUpdate &) = delete;
	SetDefaultSortOrderUpdate &operator=(const SetDefaultSortOrderUpdate &) = delete;
	SetDefaultSortOrderUpdate(SetDefaultSortOrderUpdate &&) = default;
	SetDefaultSortOrderUpdate &operator=(SetDefaultSortOrderUpdate &&) = default;

public:
	static SetDefaultSortOrderUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	int64_t sort_order_id;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
