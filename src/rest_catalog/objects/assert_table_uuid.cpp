
#include "rest_catalog/objects/assert_table_uuid.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssertTableUUID::AssertTableUUID() {
}

AssertTableUUID AssertTableUUID::FromJSON(yyjson_val *obj) {
	AssertTableUUID res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string AssertTableUUID::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "AssertTableUUID required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto uuid_val = yyjson_obj_get(obj, "uuid");
	if (!uuid_val) {
		return "AssertTableUUID required property 'uuid' is missing";
	} else {
		if (yyjson_is_str(uuid_val)) {
			uuid = yyjson_get_str(uuid_val);
		} else {
			return StringUtil::Format("AssertTableUUID property 'uuid' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(uuid_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
