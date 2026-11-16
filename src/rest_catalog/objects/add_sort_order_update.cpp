
#include "rest_catalog/objects/add_sort_order_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AddSortOrderUpdate::AddSortOrderUpdate() {
}

AddSortOrderUpdate AddSortOrderUpdate::FromJSON(JSONValue obj) {
	AddSortOrderUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AddSortOrderUpdate AddSortOrderUpdate::Copy() const {
	AddSortOrderUpdate res;
	res.base_update = base_update.Copy();
	res.sort_order = sort_order.Copy();
	return res;
}

string AddSortOrderUpdate::TryFromJSON(JSONValue obj) {
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
			    "AddSortOrderUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "add-sort-order") {
			return "AddSortOrderUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "AddSortOrderUpdate required property 'action' is missing";
	}
	auto sort_order_val = obj.GetMember("sort-order");
	if (!sort_order_val.IsValid()) {
		return "AddSortOrderUpdate required property 'sort-order' is missing";
	} else {
		error = sort_order.TryFromJSON(sort_order_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void AddSortOrderUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: sort-order
	auto sort_order_json = sort_order.ToJSON(writer);
	obj.Add("sort-order", sort_order_json);
}

JSONMutableValue AddSortOrderUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
