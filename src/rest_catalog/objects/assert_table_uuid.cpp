
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

AssertTableUUID AssertTableUUID::Copy() const {
	AssertTableUUID res;
	res.type = type;
	res.uuid = uuid;
	return res;
}

string AssertTableUUID::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "AssertTableUUID required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format("AssertTableUUID property 'type' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(type_val));
		}
		if (!yyjson_is_null(type_val) && type != "assert-table-uuid") {
			return "AssertTableUUID property 'type' does not match its required const value";
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
	return "";
}

void AssertTableUUID::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_obj_add_strcpy(doc, obj, "type", type.c_str());

	// Serialize: uuid
	yyjson_mut_obj_add_strcpy(doc, obj, "uuid", uuid.c_str());
}

yyjson_mut_val *AssertTableUUID::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
