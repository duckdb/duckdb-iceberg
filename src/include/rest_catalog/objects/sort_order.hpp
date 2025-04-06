
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/sort_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SortOrder {
public:
	SortOrder() {
	}

public:
	static SortOrder FromJSON(yyjson_val *obj) {
		SortOrder res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		auto order_id_val = yyjson_obj_get(obj, "order_id");
		if (!order_id_val) {
			return "SortOrder required property 'order_id' is missing";
		} else {
			order_id = yyjson_get_sint(order_id_val);
		}
		auto fields_val = yyjson_obj_get(obj, "fields");
		if (!fields_val) {
			return "SortOrder required property 'fields' is missing";
		} else {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(fields_val, idx, max, val) {
				SortField tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				fields.push_back(tmp);
			}
		}
		return string();
	}

public:
	int64_t order_id;
	vector<SortField> fields;
};

} // namespace rest_api_objects
} // namespace duckdb
