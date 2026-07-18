
#include "rest_catalog/objects/remove_snapshots_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemoveSnapshotsUpdate::RemoveSnapshotsUpdate() {
}

RemoveSnapshotsUpdate RemoveSnapshotsUpdate::FromJSON(yyjson_val *obj) {
	RemoveSnapshotsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RemoveSnapshotsUpdate RemoveSnapshotsUpdate::Copy() const {
	RemoveSnapshotsUpdate res;
	res.base_update = base_update.Copy();
	res.snapshot_ids.reserve(snapshot_ids.size());
	for (auto &item : snapshot_ids) {
		res.snapshot_ids.emplace_back(item);
	}
	return res;
}

string RemoveSnapshotsUpdate::TryFromJSON(yyjson_val *obj) {
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
			    "RemoveSnapshotsUpdate property 'action_refinement' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_refinement_val));
		}
		if (!yyjson_is_null(action_refinement_val) && action_refinement != "remove-snapshots") {
			return "RemoveSnapshotsUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "RemoveSnapshotsUpdate required property 'action' is missing";
	}
	auto snapshot_ids_val = yyjson_obj_get(obj, "snapshot-ids");
	if (!snapshot_ids_val) {
		return "RemoveSnapshotsUpdate required property 'snapshot-ids' is missing";
	} else {
		if (yyjson_is_arr(snapshot_ids_val)) {
			size_t snapshot_ids_idx, snapshot_ids_max;
			yyjson_val *snapshot_ids_item_val;
			yyjson_arr_foreach(snapshot_ids_val, snapshot_ids_idx, snapshot_ids_max, snapshot_ids_item_val) {
				int64_t snapshot_ids_item;
				if (yyjson_is_sint(snapshot_ids_item_val)) {
					snapshot_ids_item = yyjson_get_sint(snapshot_ids_item_val);
				} else if (yyjson_is_uint(snapshot_ids_item_val)) {
					snapshot_ids_item = yyjson_get_uint(snapshot_ids_item_val);
				} else {
					return StringUtil::Format("RemoveSnapshotsUpdate property 'snapshot_ids_item' is not of type "
					                          "'integer', found '%s' instead",
					                          yyjson_get_type_desc(snapshot_ids_item_val));
				}
				snapshot_ids.emplace_back(std::move(snapshot_ids_item));
			}
		} else {
			return StringUtil::Format(
			    "RemoveSnapshotsUpdate property 'snapshot_ids' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(snapshot_ids_val));
		}
	}
	return "";
}

void RemoveSnapshotsUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: snapshot-ids
	yyjson_mut_val *snapshot_ids_arr = yyjson_mut_arr(doc);
	for (const auto &item : snapshot_ids) {
		yyjson_mut_val *item_val = yyjson_mut_sint(doc, item);
		yyjson_mut_arr_append(snapshot_ids_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "snapshot-ids", snapshot_ids_arr);
}

yyjson_mut_val *RemoveSnapshotsUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
