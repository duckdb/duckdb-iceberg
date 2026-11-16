
#include "rest_catalog/objects/assert_view_uuid.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AssertViewUUID::AssertViewUUID() {
}

AssertViewUUID AssertViewUUID::FromJSON(JSONValue obj) {
	AssertViewUUID res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AssertViewUUID AssertViewUUID::Copy() const {
	AssertViewUUID res;
	res.type = type;
	res.uuid = uuid;
	return res;
}

string AssertViewUUID::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "AssertViewUUID required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("AssertViewUUID property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "assert-view-uuid") {
			return "AssertViewUUID property 'type' does not match its required const value";
		}
	}
	auto uuid_val = obj.GetMember("uuid");
	if (!uuid_val.IsValid()) {
		return "AssertViewUUID required property 'uuid' is missing";
	} else {
		if (json_utils::IsString(uuid_val)) {
			uuid = json_utils::GetString(uuid_val);
		} else {
			return StringUtil::Format("AssertViewUUID property 'uuid' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(uuid_val).c_str());
		}
	}
	return "";
}

void AssertViewUUID::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: uuid
	auto uuid_json = writer.CreateString(uuid);
	obj.Add("uuid", uuid_json);
}

JSONMutableValue AssertViewUUID::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
