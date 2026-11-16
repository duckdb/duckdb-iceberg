
#include "rest_catalog/objects/add_schema_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AddSchemaUpdate::AddSchemaUpdate() {
}

AddSchemaUpdate AddSchemaUpdate::FromJSON(JSONValue obj) {
	AddSchemaUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AddSchemaUpdate AddSchemaUpdate::Copy() const {
	AddSchemaUpdate res;
	res.base_update = base_update.Copy();
	res.schema = schema.Copy();
	if (last_column_id.has_value()) {
		res.last_column_id.emplace();
		(*res.last_column_id) = (*last_column_id);
	}
	return res;
}

string AddSchemaUpdate::TryFromJSON(JSONValue obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto action_refinement_val = obj.GetMember("action");
	if (action_refinement_val.IsValid()) {
		string action_refinement;
		if (json_utils::IsString(action_refinement_val)) {
			action_refinement = json_utils::GetString(action_refinement_val);
		} else {
			return StringUtil::Format(
			    "AddSchemaUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "add-schema") {
			return "AddSchemaUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "AddSchemaUpdate required property 'action' is missing";
	}
	auto schema_val = obj.GetMember("schema");
	if (!schema_val.IsValid()) {
		return "AddSchemaUpdate required property 'schema' is missing";
	} else {
		error = schema.TryFromJSON(schema_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto last_column_id_val = obj.GetMember("last-column-id");
	if (last_column_id_val.IsValid()) {
		int32_t last_column_id_tmp;
		if (json_utils::IsInteger(last_column_id_val)) {
			last_column_id_tmp = json_utils::GetSignedInteger(last_column_id_val);
		} else {
			return StringUtil::Format(
			    "AddSchemaUpdate property 'last_column_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(last_column_id_val).c_str());
		}
		last_column_id = std::move(last_column_id_tmp);
	}
	return "";
}

void AddSchemaUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: schema
	auto schema_json = schema.ToJSON(writer);
	obj.Add("schema", schema_json);

	// Serialize: last-column-id
	if (last_column_id.has_value()) {
		auto &last_column_id_value = *last_column_id;
		auto last_column_id_json = writer.CreateSignedInteger(last_column_id_value);
		obj.Add("last-column-id", last_column_id_json);
	}
}

JSONMutableValue AddSchemaUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
