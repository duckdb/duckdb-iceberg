
#include "rest_catalog/objects/schema.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Schema::Schema() {
}
Schema::Object1::Object1() {
}

Schema::Object1 Schema::Object1::FromJSON(JSONValue obj) {
	Object1 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Schema::Object1 Schema::Object1::Copy() const {
	Object1 res;
	if (schema_id.has_value()) {
		res.schema_id.emplace();
		(*res.schema_id) = (*schema_id);
	}
	if (identifier_field_ids.has_value()) {
		res.identifier_field_ids.emplace();
		(*res.identifier_field_ids).reserve((*identifier_field_ids).size());
		for (auto &item : (*identifier_field_ids)) {
			(*res.identifier_field_ids).emplace_back(item);
		}
	}
	return res;
}

string Schema::Object1::TryFromJSON(JSONValue obj) {
	string error;
	auto schema_id_val = obj.GetMember("schema-id");
	if (schema_id_val.IsValid()) {
		int32_t schema_id_tmp;
		if (json_utils::IsInteger(schema_id_val)) {
			schema_id_tmp = json_utils::GetSignedInteger(schema_id_val);
		} else {
			return StringUtil::Format("Object1 property 'schema_id_tmp' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(schema_id_val).c_str());
		}
		schema_id = std::move(schema_id_tmp);
	}
	auto identifier_field_ids_val = obj.GetMember("identifier-field-ids");
	if (identifier_field_ids_val.IsValid()) {
		vector<int32_t> identifier_field_ids_tmp;
		if (identifier_field_ids_val.IsArray()) {
			identifier_field_ids_val.IterateArray([&](JSONValue identifier_field_ids_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				int32_t identifier_field_ids_tmp_item;
				if (json_utils::IsInteger(identifier_field_ids_tmp_item_val)) {
					identifier_field_ids_tmp_item = json_utils::GetSignedInteger(identifier_field_ids_tmp_item_val);
				} else {
					error = StringUtil::Format(
					    "Object1 property 'identifier_field_ids_tmp_item' is not of type 'integer', found %s instead",
					    json_utils::GetTypeDescription(identifier_field_ids_tmp_item_val).c_str());
					return;
				}
				identifier_field_ids_tmp.emplace_back(std::move(identifier_field_ids_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "Object1 property 'identifier_field_ids_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(identifier_field_ids_val).c_str());
		}
		identifier_field_ids = std::move(identifier_field_ids_tmp);
	}
	return "";
}

void Schema::Object1::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: schema-id
	if (schema_id.has_value()) {
		auto &schema_id_value = *schema_id;
		auto schema_id_json = writer.CreateSignedInteger(schema_id_value);
		obj.Add("schema-id", schema_id_json);
	}

	// Serialize: identifier-field-ids
	if (identifier_field_ids.has_value()) {
		auto &identifier_field_ids_value = *identifier_field_ids;
		auto identifier_field_ids_json = writer.CreateArray();
		for (const auto &identifier_field_ids_json_item : identifier_field_ids_value) {
			auto identifier_field_ids_json_item_json = writer.CreateSignedInteger(identifier_field_ids_json_item);
			identifier_field_ids_json.Append(identifier_field_ids_json_item_json);
		}
		obj.Add("identifier-field-ids", identifier_field_ids_json);
	}
}

JSONMutableValue Schema::Object1::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

Schema Schema::FromJSON(JSONValue obj) {
	Schema res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Schema Schema::Copy() const {
	Schema res;
	res.struct_type = struct_type.Copy();
	res.object_1 = object_1.Copy();
	return res;
}

string Schema::TryFromJSON(JSONValue obj) {
	string error;
	error = struct_type.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	error = object_1.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return "";
}

void Schema::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: StructType
	struct_type.PopulateJSON(writer, obj);

	// Serialize base class: Object1
	object_1.PopulateJSON(writer, obj);
}

JSONMutableValue Schema::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
