
#include "rest_catalog/objects/add_partition_spec_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AddPartitionSpecUpdate::AddPartitionSpecUpdate() {
}

AddPartitionSpecUpdate AddPartitionSpecUpdate::FromJSON(JSONValue obj) {
	AddPartitionSpecUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AddPartitionSpecUpdate AddPartitionSpecUpdate::Copy() const {
	AddPartitionSpecUpdate res;
	res.base_update = base_update.Copy();
	res.spec = spec.Copy();
	return res;
}

string AddPartitionSpecUpdate::TryFromJSON(JSONValue obj) {
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
			    "AddPartitionSpecUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "add-spec") {
			return "AddPartitionSpecUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "AddPartitionSpecUpdate required property 'action' is missing";
	}
	auto spec_val = obj.GetMember("spec");
	if (!spec_val.IsValid()) {
		return "AddPartitionSpecUpdate required property 'spec' is missing";
	} else {
		error = spec.TryFromJSON(spec_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void AddPartitionSpecUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: spec
	auto spec_json = spec.ToJSON(writer);
	obj.Add("spec", spec_json);
}

JSONMutableValue AddPartitionSpecUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
