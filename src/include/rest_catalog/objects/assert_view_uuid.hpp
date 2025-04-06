
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertViewUUID {
public:
	AssertViewUUID() {
	}

public:
	static AssertViewUUID FromJSON(yyjson_val *obj) {
		AssertViewUUID res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
			return "AssertViewUUID required property 'type' is missing";
		} else {
			type = yyjson_get_str(type_val);
		}

		auto uuid_val = yyjson_obj_get(obj, "uuid");
		if (!uuid_val) {
			return "AssertViewUUID required property 'uuid' is missing";
		} else {
			uuid = yyjson_get_str(uuid_val);
		}

		return string();
	}

public:
public:
	string type;
	string uuid;
};

} // namespace rest_api_objects
} // namespace duckdb
