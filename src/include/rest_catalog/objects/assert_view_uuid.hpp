#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertViewUUID {
public:
	static AssertViewUUID FromJSON(yyjson_val *obj) {
		AssertViewUUID result;

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		else {
			throw IOException("AssertViewUUID required property 'type' is missing");
		}

		auto uuid_val = yyjson_obj_get(obj, "uuid");
		if (uuid_val) {
			result.uuid = yyjson_get_str(uuid_val);
		}
		else {
			throw IOException("AssertViewUUID required property 'uuid' is missing");
		}

		return result;
	}

public:
	string type;
	string uuid;
};
} // namespace rest_api_objects
} // namespace duckdb