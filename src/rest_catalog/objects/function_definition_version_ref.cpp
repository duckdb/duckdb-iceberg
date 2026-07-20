
#include "rest_catalog/objects/function_definition_version_ref.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FunctionDefinitionVersionRef::FunctionDefinitionVersionRef() {
}

FunctionDefinitionVersionRef FunctionDefinitionVersionRef::FromJSON(yyjson_val *obj) {
	FunctionDefinitionVersionRef res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionDefinitionVersionRef FunctionDefinitionVersionRef::Copy() const {
	FunctionDefinitionVersionRef res;
	res.definition_id = definition_id;
	res.version_id = version_id;
	return res;
}

string FunctionDefinitionVersionRef::TryFromJSON(yyjson_val *obj) {
	string error;
	auto definition_id_val = yyjson_obj_get(obj, "definition-id");
	if (!definition_id_val) {
		return "FunctionDefinitionVersionRef required property 'definition-id' is missing";
	} else {
		if (yyjson_is_str(definition_id_val)) {
			definition_id = yyjson_get_str(definition_id_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersionRef property 'definition_id' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(definition_id_val));
		}
	}
	auto version_id_val = yyjson_obj_get(obj, "version-id");
	if (!version_id_val) {
		return "FunctionDefinitionVersionRef required property 'version-id' is missing";
	} else {
		if (yyjson_is_int(version_id_val)) {
			version_id = yyjson_get_int(version_id_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersionRef property 'version_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(version_id_val));
		}
	}
	return "";
}

void FunctionDefinitionVersionRef::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: definition-id
	yyjson_mut_obj_add_strcpy(doc, obj, "definition-id", definition_id.c_str());

	// Serialize: version-id
	yyjson_mut_obj_add_int(doc, obj, "version-id", version_id);
}

yyjson_mut_val *FunctionDefinitionVersionRef::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
