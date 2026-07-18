
#include "rest_catalog/objects/assert_current_schema_id.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssertCurrentSchemaId::AssertCurrentSchemaId() {
}

AssertCurrentSchemaId AssertCurrentSchemaId::FromJSON(yyjson_val *obj) {
	AssertCurrentSchemaId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AssertCurrentSchemaId AssertCurrentSchemaId::Copy() const {
	AssertCurrentSchemaId res;
	res.type = type;
	res.current_schema_id = current_schema_id;
	return res;
}

string AssertCurrentSchemaId::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "AssertCurrentSchemaId required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format(
			    "AssertCurrentSchemaId property 'type' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(type_val));
		}
		if (!yyjson_is_null(type_val) && type != "assert-current-schema-id") {
			return "AssertCurrentSchemaId property 'type' does not match its required const value";
		}
	}
	auto current_schema_id_val = yyjson_obj_get(obj, "current-schema-id");
	if (!current_schema_id_val) {
		return "AssertCurrentSchemaId required property 'current-schema-id' is missing";
	} else {
		if (yyjson_is_int(current_schema_id_val)) {
			current_schema_id = yyjson_get_int(current_schema_id_val);
		} else {
			return StringUtil::Format(
			    "AssertCurrentSchemaId property 'current_schema_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(current_schema_id_val));
		}
	}
	return "";
}

void AssertCurrentSchemaId::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_obj_add_strcpy(doc, obj, "type", type.c_str());

	// Serialize: current-schema-id
	yyjson_mut_obj_add_int(doc, obj, "current-schema-id", current_schema_id);
}

yyjson_mut_val *AssertCurrentSchemaId::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
