
#include "rest_catalog/objects/table_identifier.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

TableIdentifier::TableIdentifier() {
}

TableIdentifier TableIdentifier::FromJSON(JSONValue obj) {
	TableIdentifier res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

TableIdentifier TableIdentifier::Copy() const {
	TableIdentifier res;
	res._namespace = _namespace.Copy();
	res.name = name;
	return res;
}

string TableIdentifier::TryFromJSON(JSONValue obj) {
	string error;
	auto _namespace_val = obj.GetMember("namespace");
	if (!_namespace_val.IsValid()) {
		return "TableIdentifier required property 'namespace' is missing";
	} else {
		error = _namespace.TryFromJSON(_namespace_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto name_val = obj.GetMember("name");
	if (!name_val.IsValid()) {
		return "TableIdentifier required property 'name' is missing";
	} else {
		if (name_val.IsNull()) {
			return "TableIdentifier property 'name' is not nullable, but is 'null'";
		} else if (json_utils::IsString(name_val)) {
			name = json_utils::GetString(name_val);
		} else {
			return StringUtil::Format("TableIdentifier property 'name' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(name_val).c_str());
		}
	}
	return "";
}

void TableIdentifier::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: namespace
	auto _namespace_json = _namespace.ToJSON(writer);
	obj.Add("namespace", _namespace_json);

	// Serialize: name
	auto name_json = writer.CreateString(name);
	obj.Add("name", name_json);
}

JSONMutableValue TableIdentifier::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
