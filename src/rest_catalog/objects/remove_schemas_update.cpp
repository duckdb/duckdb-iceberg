
#include "rest_catalog/objects/remove_schemas_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

RemoveSchemasUpdate::RemoveSchemasUpdate() {
}

RemoveSchemasUpdate RemoveSchemasUpdate::FromJSON(JSONValue obj) {
	RemoveSchemasUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RemoveSchemasUpdate RemoveSchemasUpdate::Copy() const {
	RemoveSchemasUpdate res;
	res.base_update = base_update.Copy();
	res.schema_ids.reserve(schema_ids.size());
	for (auto &item : schema_ids) {
		res.schema_ids.emplace_back(item);
	}
	return res;
}

string RemoveSchemasUpdate::TryFromJSON(JSONValue obj) {
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
			    "RemoveSchemasUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "remove-schemas") {
			return "RemoveSchemasUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "RemoveSchemasUpdate required property 'action' is missing";
	}
	auto schema_ids_val = obj.GetMember("schema-ids");
	if (!schema_ids_val.IsValid()) {
		return "RemoveSchemasUpdate required property 'schema-ids' is missing";
	} else {
		if (schema_ids_val.IsArray()) {
			schema_ids_val.IterateArray([&](JSONValue schema_ids_item_val) {
				if (!error.empty()) {
					return;
				}
				int32_t schema_ids_item;
				if (json_utils::IsInteger(schema_ids_item_val)) {
					schema_ids_item = json_utils::GetSignedInteger(schema_ids_item_val);
				} else {
					error = StringUtil::Format(
					    "RemoveSchemasUpdate property 'schema_ids_item' is not of type 'integer', found %s instead",
					    json_utils::GetTypeDescription(schema_ids_item_val).c_str());
					return;
				}
				schema_ids.emplace_back(std::move(schema_ids_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "RemoveSchemasUpdate property 'schema_ids' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(schema_ids_val).c_str());
		}
	}
	return "";
}

void RemoveSchemasUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: schema-ids
	auto schema_ids_json = writer.CreateArray();
	for (const auto &schema_ids_json_item : schema_ids) {
		auto schema_ids_json_item_json = writer.CreateSignedInteger(schema_ids_json_item);
		schema_ids_json.Append(schema_ids_json_item_json);
	}
	obj.Add("schema-ids", schema_ids_json);
}

JSONMutableValue RemoveSchemasUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
