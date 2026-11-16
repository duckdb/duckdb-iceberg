
#include "rest_catalog/objects/function_sqlrepresentation.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionSQLRepresentation::FunctionSQLRepresentation() {
}

FunctionSQLRepresentation FunctionSQLRepresentation::FromJSON(JSONValue obj) {
	FunctionSQLRepresentation res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionSQLRepresentation FunctionSQLRepresentation::Copy() const {
	FunctionSQLRepresentation res;
	res.type = type;
	res.dialect = dialect;
	res.sql = sql;
	return res;
}

string FunctionSQLRepresentation::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "FunctionSQLRepresentation required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format(
			    "FunctionSQLRepresentation property 'type' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "sql") {
			return "FunctionSQLRepresentation property 'type' does not match its required const value";
		}
	}
	auto dialect_val = obj.GetMember("dialect");
	if (!dialect_val.IsValid()) {
		return "FunctionSQLRepresentation required property 'dialect' is missing";
	} else {
		if (json_utils::IsString(dialect_val)) {
			dialect = json_utils::GetString(dialect_val);
		} else {
			return StringUtil::Format(
			    "FunctionSQLRepresentation property 'dialect' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(dialect_val).c_str());
		}
	}
	auto sql_val = obj.GetMember("sql");
	if (!sql_val.IsValid()) {
		return "FunctionSQLRepresentation required property 'sql' is missing";
	} else {
		if (json_utils::IsString(sql_val)) {
			sql = json_utils::GetString(sql_val);
		} else {
			return StringUtil::Format(
			    "FunctionSQLRepresentation property 'sql' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(sql_val).c_str());
		}
	}
	return "";
}

void FunctionSQLRepresentation::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: dialect
	auto dialect_json = writer.CreateString(dialect);
	obj.Add("dialect", dialect_json);

	// Serialize: sql
	auto sql_json = writer.CreateString(sql);
	obj.Add("sql", sql_json);
}

JSONMutableValue FunctionSQLRepresentation::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
