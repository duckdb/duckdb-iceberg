
#include "rest_catalog/objects/remote_sign_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

RemoteSignResult::RemoteSignResult() {
}

RemoteSignResult RemoteSignResult::FromJSON(JSONValue obj) {
	RemoteSignResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RemoteSignResult RemoteSignResult::Copy() const {
	RemoteSignResult res;
	res.uri = uri;
	res.headers = headers.Copy();
	return res;
}

string RemoteSignResult::TryFromJSON(JSONValue obj) {
	string error;
	auto uri_val = obj.GetMember("uri");
	if (!uri_val.IsValid()) {
		return "RemoteSignResult required property 'uri' is missing";
	} else {
		if (json_utils::IsString(uri_val)) {
			uri = json_utils::GetString(uri_val);
		} else {
			return StringUtil::Format("RemoteSignResult property 'uri' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(uri_val).c_str());
		}
	}
	auto headers_val = obj.GetMember("headers");
	if (!headers_val.IsValid()) {
		return "RemoteSignResult required property 'headers' is missing";
	} else {
		error = headers.TryFromJSON(headers_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void RemoteSignResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: uri
	auto uri_json = writer.CreateString(uri);
	obj.Add("uri", uri_json);

	// Serialize: headers
	auto headers_json = headers.ToJSON(writer);
	obj.Add("headers", headers_json);
}

JSONMutableValue RemoteSignResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
