
#include "rest_catalog/objects/set_default_sort_order_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetDefaultSortOrderUpdate::SetDefaultSortOrderUpdate() {
}

SetDefaultSortOrderUpdate SetDefaultSortOrderUpdate::FromJSON(yyjson_val *obj) {
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

string SetDefaultSortOrderUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto action_refinement_val = yyjson_obj_get(obj, "action");
	if (action_refinement_val) {
		string action_refinement;
		if (yyjson_is_str(action_refinement_val)) {
			action_refinement = yyjson_get_str(action_refinement_val);
		} else {
			return StringUtil::Format(
			    "SetDefaultSortOrderUpdate property 'action_refinement' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_refinement_val));
		}
		if (!yyjson_is_null(action_refinement_val) && action_refinement != "set-default-sort-order") {
			return "SetDefaultSortOrderUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "SetDefaultSortOrderUpdate required property 'action' is missing";
	}
	auto sort_order_id_val = yyjson_obj_get(obj, "sort-order-id");
	if (!sort_order_id_val) {
		return "SetDefaultSortOrderUpdate required property 'sort-order-id' is missing";
	} else {
		if (yyjson_is_int(sort_order_id_val)) {
			sort_order_id = yyjson_get_int(sort_order_id_val);
		} else {
			return StringUtil::Format(
			    "SetDefaultSortOrderUpdate property 'sort_order_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(sort_order_id_val));
		}
	}
	return "";
}

void SetDefaultSortOrderUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: sort-order-id
	yyjson_mut_obj_add_int(doc, obj, "sort-order-id", sort_order_id);
}

yyjson_mut_val *SetDefaultSortOrderUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
