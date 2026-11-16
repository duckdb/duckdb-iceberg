
#include "rest_catalog/objects/sqlview_representation.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SQLViewRepresentation::SQLViewRepresentation() {
}

SQLViewRepresentation SQLViewRepresentation::FromJSON(JSONValue obj) {
	SQLViewRepresentation res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SQLViewRepresentation SQLViewRepresentation::Copy() const {
	SQLViewRepresentation res;
	res.type = type;
	res.sql = sql;
	res.dialect = dialect;
	return res;
}

string SQLViewRepresentation::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "SQLViewRepresentation required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("SQLViewRepresentation property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
	}
	auto sql_val = obj.GetMember("sql");
	if (!sql_val.IsValid()) {
		return "SQLViewRepresentation required property 'sql' is missing";
	} else {
		if (json_utils::IsString(sql_val)) {
			sql = json_utils::GetString(sql_val);
		} else {
			return StringUtil::Format("SQLViewRepresentation property 'sql' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(sql_val).c_str());
		}
	}
	auto dialect_val = obj.GetMember("dialect");
	if (!dialect_val.IsValid()) {
		return "SQLViewRepresentation required property 'dialect' is missing";
	} else {
		if (json_utils::IsString(dialect_val)) {
			dialect = json_utils::GetString(dialect_val);
		} else {
			return StringUtil::Format(
			    "SQLViewRepresentation property 'dialect' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(dialect_val).c_str());
		}
	}
	return "";
}

void SQLViewRepresentation::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: sql
	auto sql_json = writer.CreateString(sql);
	obj.Add("sql", sql_json);

	// Serialize: dialect
	auto dialect_json = writer.CreateString(dialect);
	obj.Add("dialect", dialect_json);
}

JSONMutableValue SQLViewRepresentation::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
