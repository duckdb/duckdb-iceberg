
#include "rest_catalog/objects/remote_sign_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemoteSignResult::RemoteSignResult() {
}

RemoteSignResult RemoteSignResult::FromJSON(yyjson_val *obj) {
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

string RemoteSignResult::TryFromJSON(yyjson_val *obj) {
	string error;
	auto uri_val = yyjson_obj_get(obj, "uri");
	if (!uri_val) {
		return "RemoteSignResult required property 'uri' is missing";
	} else {
		if (yyjson_is_str(uri_val)) {
			uri = yyjson_get_str(uri_val);
		} else {
			return StringUtil::Format("RemoteSignResult property 'uri' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(uri_val));
		}
	}
	auto headers_val = yyjson_obj_get(obj, "headers");
	if (!headers_val) {
		return "RemoteSignResult required property 'headers' is missing";
	} else {
		error = headers.TryFromJSON(headers_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void RemoteSignResult::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: uri
	yyjson_mut_obj_add_strcpy(doc, obj, "uri", uri.c_str());

	// Serialize: headers
	yyjson_mut_val *headers_val = headers.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "headers", headers_val);
}

yyjson_mut_val *RemoteSignResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
