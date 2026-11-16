
#include "rest_catalog/objects/assert_default_spec_id.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AssertDefaultSpecId::AssertDefaultSpecId() {
}

AssertDefaultSpecId AssertDefaultSpecId::FromJSON(JSONValue obj) {
	AssertDefaultSpecId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AssertDefaultSpecId AssertDefaultSpecId::Copy() const {
	AssertDefaultSpecId res;
	res.type = type;
	res.default_spec_id = default_spec_id;
	return res;
}

string AssertDefaultSpecId::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "AssertDefaultSpecId required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("AssertDefaultSpecId property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "assert-default-spec-id") {
			return "AssertDefaultSpecId property 'type' does not match its required const value";
		}
	}
	auto default_spec_id_val = obj.GetMember("default-spec-id");
	if (!default_spec_id_val.IsValid()) {
		return "AssertDefaultSpecId required property 'default-spec-id' is missing";
	} else {
		if (json_utils::IsInteger(default_spec_id_val)) {
			default_spec_id = json_utils::GetSignedInteger(default_spec_id_val);
		} else {
			return StringUtil::Format(
			    "AssertDefaultSpecId property 'default_spec_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(default_spec_id_val).c_str());
		}
	}
	return "";
}

void AssertDefaultSpecId::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: default-spec-id
	auto default_spec_id_json = writer.CreateSignedInteger(default_spec_id);
	obj.Add("default-spec-id", default_spec_id_json);
}

JSONMutableValue AssertDefaultSpecId::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
