
#include "rest_catalog/objects/field_labels.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FieldLabels::FieldLabels() {
}

FieldLabels FieldLabels::FromJSON(JSONValue obj) {
	FieldLabels res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FieldLabels FieldLabels::Copy() const {
	FieldLabels res;
	res.field_id = field_id;
	for (auto &entry : labels) {
		res.labels.emplace(entry.first, entry.second);
	}
	return res;
}

string FieldLabels::TryFromJSON(JSONValue obj) {
	string error;
	auto field_id_val = obj.GetMember("field-id");
	if (!field_id_val.IsValid()) {
		return "FieldLabels required property 'field-id' is missing";
	} else {
		if (json_utils::IsInteger(field_id_val)) {
			field_id = json_utils::GetSignedInteger(field_id_val);
		} else {
			return StringUtil::Format("FieldLabels property 'field_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(field_id_val).c_str());
		}
	}
	auto labels_val = obj.GetMember("labels");
	if (!labels_val.IsValid()) {
		return "FieldLabels required property 'labels' is missing";
	} else {
		if (labels_val.IsObject()) {
			labels_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error = StringUtil::Format("FieldLabels property 'tmp' is not of type 'string', found %s instead",
					                           json_utils::GetTypeDescription(val).c_str());
					return;
				}
				labels.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "FieldLabels property 'labels' is not of type 'object'";
		}
	}
	return "";
}

void FieldLabels::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: field-id
	auto field_id_json = writer.CreateSignedInteger(field_id);
	obj.Add("field-id", field_id_json);

	// Serialize: labels
	auto labels_json = writer.CreateObject();
	for (const auto &[labels_json_key, labels_json_value] : labels) {
		auto labels_json_value_json = writer.CreateString(labels_json_value);
		labels_json.Add(labels_json_key, labels_json_value_json);
	}
	obj.Add("labels", labels_json);
}

JSONMutableValue FieldLabels::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
