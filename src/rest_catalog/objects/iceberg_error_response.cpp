
#include "rest_catalog/objects/iceberg_error_response.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

IcebergErrorResponse::IcebergErrorResponse() {
}

IcebergErrorResponse IcebergErrorResponse::FromJSON(JSONValue obj) {
	IcebergErrorResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

IcebergErrorResponse IcebergErrorResponse::Copy() const {
	IcebergErrorResponse res;
	res._error = _error.Copy();
	return res;
}

string IcebergErrorResponse::TryFromJSON(JSONValue obj) {
	string error;
	auto _error_val = obj.GetMember("error");
	if (!_error_val.IsValid()) {
		return "IcebergErrorResponse required property 'error' is missing";
	} else {
		error = _error.TryFromJSON(_error_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void IcebergErrorResponse::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: error
	auto _error_json = _error.ToJSON(writer);
	obj.Add("error", _error_json);
}

JSONMutableValue IcebergErrorResponse::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
