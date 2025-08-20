
#include "rest_catalog/objects/assert_default_spec_id.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssertDefaultSpecId::AssertDefaultSpecId() {
}

AssertDefaultSpecId AssertDefaultSpecId::FromJSON(yyjson_val *obj) {
	AssertDefaultSpecId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string AssertDefaultSpecId::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "AssertDefaultSpecId required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto default_spec_id_val = yyjson_obj_get(obj, "default-spec-id");
	if (!default_spec_id_val) {
		return "AssertDefaultSpecId required property 'default-spec-id' is missing";
	} else {
		if (yyjson_is_int(default_spec_id_val)) {
			default_spec_id = yyjson_get_int(default_spec_id_val);
		} else {
			return StringUtil::Format(
			    "AssertDefaultSpecId property 'default_spec_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(default_spec_id_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
