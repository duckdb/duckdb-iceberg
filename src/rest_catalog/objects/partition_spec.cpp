
#include "rest_catalog/objects/partition_spec.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

PartitionSpec::PartitionSpec() {
}

PartitionSpec PartitionSpec::FromJSON(JSONValue obj) {
	PartitionSpec res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

PartitionSpec PartitionSpec::Copy() const {
	PartitionSpec res;
	res.fields.reserve(fields.size());
	for (auto &item : fields) {
		res.fields.emplace_back(item.Copy());
	}
	if (spec_id.has_value()) {
		res.spec_id.emplace();
		(*res.spec_id) = (*spec_id);
	}
	return res;
}

string PartitionSpec::TryFromJSON(JSONValue obj) {
	string error;
	auto fields_val = obj.GetMember("fields");
	if (!fields_val.IsValid()) {
		return "PartitionSpec required property 'fields' is missing";
	} else {
		if (fields_val.IsArray()) {
			fields_val.IterateArray([&](JSONValue fields_item_val) {
				if (!error.empty()) {
					return;
				}
				PartitionField fields_item;
				error = fields_item.TryFromJSON(fields_item_val);
				if (!error.empty()) {
					return;
				}
				fields.emplace_back(std::move(fields_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("PartitionSpec property 'fields' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(fields_val).c_str());
		}
	}
	auto spec_id_val = obj.GetMember("spec-id");
	if (spec_id_val.IsValid()) {
		int32_t spec_id_tmp;
		if (json_utils::IsInteger(spec_id_val)) {
			spec_id_tmp = json_utils::GetSignedInteger(spec_id_val);
		} else {
			return StringUtil::Format("PartitionSpec property 'spec_id_tmp' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(spec_id_val).c_str());
		}
		spec_id = std::move(spec_id_tmp);
	}
	return "";
}

void PartitionSpec::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: fields
	auto fields_json = writer.CreateArray();
	for (const auto &fields_json_item : fields) {
		auto fields_json_item_json = fields_json_item.ToJSON(writer);
		fields_json.Append(fields_json_item_json);
	}
	obj.Add("fields", fields_json);

	// Serialize: spec-id
	if (spec_id.has_value()) {
		auto &spec_id_value = *spec_id;
		auto spec_id_json = writer.CreateSignedInteger(spec_id_value);
		obj.Add("spec-id", spec_id_json);
	}
}

JSONMutableValue PartitionSpec::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
