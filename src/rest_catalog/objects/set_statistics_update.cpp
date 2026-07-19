
#include "rest_catalog/objects/set_statistics_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetStatisticsUpdate::SetStatisticsUpdate() {
}

SetStatisticsUpdate SetStatisticsUpdate::FromJSON(yyjson_val *obj) {
	SetStatisticsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetStatisticsUpdate SetStatisticsUpdate::Copy() const {
	SetStatisticsUpdate res;
	res.base_update = base_update.Copy();
	res.statistics = statistics.Copy();
	if (snapshot_id.has_value()) {
		res.snapshot_id.emplace();
		(*res.snapshot_id) = (*snapshot_id);
	}
	return res;
}

string SetStatisticsUpdate::TryFromJSON(yyjson_val *obj) {
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
			    "SetStatisticsUpdate property 'action_refinement' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_refinement_val));
		}
		if (!yyjson_is_null(action_refinement_val) && action_refinement != "set-statistics") {
			return "SetStatisticsUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "SetStatisticsUpdate required property 'action' is missing";
	}
	auto statistics_val = yyjson_obj_get(obj, "statistics");
	if (!statistics_val) {
		return "SetStatisticsUpdate required property 'statistics' is missing";
	} else {
		error = statistics.TryFromJSON(statistics_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (snapshot_id_val) {
		int64_t snapshot_id_tmp;
		if (yyjson_is_sint(snapshot_id_val)) {
			snapshot_id_tmp = yyjson_get_sint(snapshot_id_val);
		} else if (yyjson_is_uint(snapshot_id_val)) {
			snapshot_id_tmp = yyjson_get_uint(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "SetStatisticsUpdate property 'snapshot_id_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(snapshot_id_val));
		}
		snapshot_id = std::move(snapshot_id_tmp);
	}
	return "";
}

void SetStatisticsUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: statistics
	yyjson_mut_val *statistics_val = statistics.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "statistics", statistics_val);

	// Serialize: snapshot-id
	if (snapshot_id.has_value()) {
		auto &snapshot_id_value = *snapshot_id;
		yyjson_mut_obj_add_sint(doc, obj, "snapshot-id", snapshot_id_value);
	}
}

yyjson_mut_val *SetStatisticsUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
