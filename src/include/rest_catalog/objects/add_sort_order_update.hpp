
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/sort_order.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AddSortOrderUpdate {
public:
	AddSortOrderUpdate();
	AddSortOrderUpdate(const AddSortOrderUpdate &) = delete;
	AddSortOrderUpdate &operator=(const AddSortOrderUpdate &) = delete;
	AddSortOrderUpdate(AddSortOrderUpdate &&) = default;
	AddSortOrderUpdate &operator=(AddSortOrderUpdate &&) = default;

public:
	static AddSortOrderUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	SortOrder sort_order;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
