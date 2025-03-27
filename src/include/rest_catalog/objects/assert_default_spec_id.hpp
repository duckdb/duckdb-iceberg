#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_requirement.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertDefaultSpecId {
public:
	static AssertDefaultSpecId FromJSON(yyjson_val *obj) {
		AssertDefaultSpecId result;

		// Parse TableRequirement fields
		result.table_requirement = TableRequirement::FromJSON(obj);

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}

		auto default_spec_id_val = yyjson_obj_get(obj, "default-spec-id");
		if (default_spec_id_val) {
			result.default_spec_id = yyjson_get_sint(default_spec_id_val);
		}
		else {
			throw IOException("AssertDefaultSpecId required property 'default-spec-id' is missing");
		}

		return result;
	}

public:
	TableRequirement table_requirement;
	string type;
	int64_t default_spec_id;
};
} // namespace rest_api_objects
} // namespace duckdb