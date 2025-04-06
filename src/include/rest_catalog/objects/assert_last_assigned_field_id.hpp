
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_requirement.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertLastAssignedFieldId {
public:
	AssertLastAssignedFieldId() {
	}

public:
	static AssertLastAssignedFieldId FromJSON(yyjson_val *obj) {
		AssertLastAssignedFieldId res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = table_requirement.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto last_assigned_field_id_val = yyjson_obj_get(obj, "last_assigned_field_id");
		if (!last_assigned_field_id_val) {
			return "AssertLastAssignedFieldId required property 'last_assigned_field_id' is missing";
		} else {
			last_assigned_field_id = yyjson_get_sint(last_assigned_field_id_val);
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			type = yyjson_get_str(type_val);
		}

		return string();
	}

public:
	TableRequirement table_requirement;

public:
	int64_t last_assigned_field_id;
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
