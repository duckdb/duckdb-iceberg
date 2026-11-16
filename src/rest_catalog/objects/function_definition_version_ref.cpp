
#include "rest_catalog/objects/function_definition_version_ref.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionDefinitionVersionRef::FunctionDefinitionVersionRef() {
}

FunctionDefinitionVersionRef FunctionDefinitionVersionRef::FromJSON(JSONValue obj) {
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

string FunctionDefinitionVersionRef::TryFromJSON(JSONValue obj) {
	string error;
	auto definition_id_val = obj.GetMember("definition-id");
	if (!definition_id_val.IsValid()) {
		return "FunctionDefinitionVersionRef required property 'definition-id' is missing";
	} else {
		if (json_utils::IsString(definition_id_val)) {
			definition_id = json_utils::GetString(definition_id_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersionRef property 'definition_id' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(definition_id_val).c_str());
		}
	}
	auto version_id_val = obj.GetMember("version-id");
	if (!version_id_val.IsValid()) {
		return "FunctionDefinitionVersionRef required property 'version-id' is missing";
	} else {
		if (json_utils::IsInteger(version_id_val)) {
			version_id = json_utils::GetSignedInteger(version_id_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersionRef property 'version_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(version_id_val).c_str());
		}
	}
	return "";
}

void FunctionDefinitionVersionRef::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: definition-id
	auto definition_id_json = writer.CreateString(definition_id);
	obj.Add("definition-id", definition_id_json);

	// Serialize: version-id
	auto version_id_json = writer.CreateSignedInteger(version_id);
	obj.Add("version-id", version_id_json);
}

JSONMutableValue FunctionDefinitionVersionRef::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
