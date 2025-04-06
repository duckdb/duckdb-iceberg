
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TableRequirement {
public:
	TableRequirement::TableRequirement() {
	}

public:
	static TableRequirement FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
		return "TableRequirement required property 'type' is missing");
		}
		result.type = yyjson_get_str(type_val);

		return string();
	}

public:
public:
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
