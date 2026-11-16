
#include "rest_catalog/objects/remove_partition_specs_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

RemovePartitionSpecsUpdate::RemovePartitionSpecsUpdate() {
}

RemovePartitionSpecsUpdate RemovePartitionSpecsUpdate::FromJSON(JSONValue obj) {
	RemovePartitionSpecsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RemovePartitionSpecsUpdate RemovePartitionSpecsUpdate::Copy() const {
	RemovePartitionSpecsUpdate res;
	res.base_update = base_update.Copy();
	res.spec_ids.reserve(spec_ids.size());
	for (auto &item : spec_ids) {
		res.spec_ids.emplace_back(item);
	}
	return res;
}

string RemovePartitionSpecsUpdate::TryFromJSON(JSONValue obj) {
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
			    "RemovePartitionSpecsUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "remove-partition-specs") {
			return "RemovePartitionSpecsUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "RemovePartitionSpecsUpdate required property 'action' is missing";
	}
	auto spec_ids_val = obj.GetMember("spec-ids");
	if (!spec_ids_val.IsValid()) {
		return "RemovePartitionSpecsUpdate required property 'spec-ids' is missing";
	} else {
		if (spec_ids_val.IsArray()) {
			spec_ids_val.IterateArray([&](JSONValue spec_ids_item_val) {
				if (!error.empty()) {
					return;
				}
				int32_t spec_ids_item;
				if (json_utils::IsInteger(spec_ids_item_val)) {
					spec_ids_item = json_utils::GetSignedInteger(spec_ids_item_val);
				} else {
					error = StringUtil::Format("RemovePartitionSpecsUpdate property 'spec_ids_item' is not of type "
					                           "'integer', found %s instead",
					                           json_utils::GetTypeDescription(spec_ids_item_val).c_str());
					return;
				}
				spec_ids.emplace_back(std::move(spec_ids_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "RemovePartitionSpecsUpdate property 'spec_ids' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(spec_ids_val).c_str());
		}
	}
	return "";
}

void RemovePartitionSpecsUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: spec-ids
	auto spec_ids_json = writer.CreateArray();
	for (const auto &spec_ids_json_item : spec_ids) {
		auto spec_ids_json_item_json = writer.CreateSignedInteger(spec_ids_json_item);
		spec_ids_json.Append(spec_ids_json_item_json);
	}
	obj.Add("spec-ids", spec_ids_json);
}

JSONMutableValue RemovePartitionSpecsUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
