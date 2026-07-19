
#include "rest_catalog/objects/function_sqlrepresentation.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FunctionSQLRepresentation::FunctionSQLRepresentation() {
}

FunctionSQLRepresentation FunctionSQLRepresentation::FromJSON(yyjson_val *obj) {
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

string FunctionSQLRepresentation::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "FunctionSQLRepresentation required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format(
			    "FunctionSQLRepresentation property 'type' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(type_val));
		}
		if (!yyjson_is_null(type_val) && type != "sql") {
			return "FunctionSQLRepresentation property 'type' does not match its required const value";
		}
	}
	auto dialect_val = yyjson_obj_get(obj, "dialect");
	if (!dialect_val) {
		return "FunctionSQLRepresentation required property 'dialect' is missing";
	} else {
		if (yyjson_is_str(dialect_val)) {
			dialect = yyjson_get_str(dialect_val);
		} else {
			return StringUtil::Format(
			    "FunctionSQLRepresentation property 'dialect' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(dialect_val));
		}
	}
	auto sql_val = yyjson_obj_get(obj, "sql");
	if (!sql_val) {
		return "FunctionSQLRepresentation required property 'sql' is missing";
	} else {
		if (yyjson_is_str(sql_val)) {
			sql = yyjson_get_str(sql_val);
		} else {
			return StringUtil::Format(
			    "FunctionSQLRepresentation property 'sql' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(sql_val));
		}
	}
	return "";
}

void FunctionSQLRepresentation::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_obj_add_strcpy(doc, obj, "type", type.c_str());

	// Serialize: dialect
	yyjson_mut_obj_add_strcpy(doc, obj, "dialect", dialect.c_str());

	// Serialize: sql
	yyjson_mut_obj_add_strcpy(doc, obj, "sql", sql.c_str());
}

yyjson_mut_val *FunctionSQLRepresentation::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
