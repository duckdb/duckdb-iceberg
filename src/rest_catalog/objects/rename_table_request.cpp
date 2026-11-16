
#include "rest_catalog/objects/rename_table_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

RenameTableRequest::RenameTableRequest() {
}

RenameTableRequest RenameTableRequest::FromJSON(JSONValue obj) {
	RenameTableRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RenameTableRequest RenameTableRequest::Copy() const {
	RenameTableRequest res;
	res.source = source.Copy();
	res.destination = destination.Copy();
	return res;
}

string RenameTableRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto source_val = obj.GetMember("source");
	if (!source_val.IsValid()) {
		return "RenameTableRequest required property 'source' is missing";
	} else {
		error = source.TryFromJSON(source_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto destination_val = obj.GetMember("destination");
	if (!destination_val.IsValid()) {
		return "RenameTableRequest required property 'destination' is missing";
	} else {
		error = destination.TryFromJSON(destination_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void RenameTableRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: source
	auto source_json = source.ToJSON(writer);
	obj.Add("source", source_json);

	// Serialize: destination
	auto destination_json = destination.ToJSON(writer);
	obj.Add("destination", destination_json);
}

JSONMutableValue RenameTableRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
