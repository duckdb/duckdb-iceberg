
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

class AssertDefaultSpecId {
public:
	AssertDefaultSpecId::AssertDefaultSpecId() {
	}

public:
	static AssertDefaultSpecId FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_table_requirement.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto default_spec_id_val = yyjson_obj_get(obj, "default_spec_id");
		if (!default_spec_id_val) {
		return "AssertDefaultSpecId required property 'default_spec_id' is missing");
		}
		result.default_spec_id = yyjson_get_sint(default_spec_id_val);

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		return string();
	}

public:
	TableRequirement table_requirement;

public:
	int64_t default_spec_id;
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
