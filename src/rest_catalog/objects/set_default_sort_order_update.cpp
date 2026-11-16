
#include "rest_catalog/objects/set_default_sort_order_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SetDefaultSortOrderUpdate::SetDefaultSortOrderUpdate() {
}

SetDefaultSortOrderUpdate SetDefaultSortOrderUpdate::FromJSON(JSONValue obj) {
	SetDefaultSortOrderUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetDefaultSortOrderUpdate SetDefaultSortOrderUpdate::Copy() const {
	SetDefaultSortOrderUpdate res;
	res.base_update = base_update.Copy();
	res.sort_order_id = sort_order_id;
	return res;
}

string SetDefaultSortOrderUpdate::TryFromJSON(JSONValue obj) {
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
			    "SetDefaultSortOrderUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "set-default-sort-order") {
			return "SetDefaultSortOrderUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "SetDefaultSortOrderUpdate required property 'action' is missing";
	}
	auto sort_order_id_val = obj.GetMember("sort-order-id");
	if (!sort_order_id_val.IsValid()) {
		return "SetDefaultSortOrderUpdate required property 'sort-order-id' is missing";
	} else {
		if (json_utils::IsInteger(sort_order_id_val)) {
			sort_order_id = json_utils::GetSignedInteger(sort_order_id_val);
		} else {
			return StringUtil::Format(
			    "SetDefaultSortOrderUpdate property 'sort_order_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(sort_order_id_val).c_str());
		}
	}
	return "";
}

void SetDefaultSortOrderUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: sort-order-id
	auto sort_order_id_json = writer.CreateSignedInteger(sort_order_id);
	obj.Add("sort-order-id", sort_order_id_json);
}

JSONMutableValue SetDefaultSortOrderUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
