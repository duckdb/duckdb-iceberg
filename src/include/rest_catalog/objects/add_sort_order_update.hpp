
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
	AddSortOrderUpdate() {
	}

public:
	static AddSortOrderUpdate FromJSON(yyjson_val *obj) {
		AddSortOrderUpdate res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_update.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto sort_order_val = yyjson_obj_get(obj, "sort_order");
		if (!sort_order_val) {
			return "AddSortOrderUpdate required property 'sort_order' is missing";
		} else {
			error = sort_order.TryFromJSON(sort_order_val);
			if (!error.empty()) {
				return error;
			}
		}

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			action = yyjson_get_str(action_val);
		}

		return string();
	}

public:
	BaseUpdate base_update;

public:
	string action;
	SortOrder sort_order;
};

} // namespace rest_api_objects
} // namespace duckdb
