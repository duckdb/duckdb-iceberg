
#include "rest_catalog/objects/upgrade_format_version_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

UpgradeFormatVersionUpdate::UpgradeFormatVersionUpdate() {
}

UpgradeFormatVersionUpdate UpgradeFormatVersionUpdate::FromJSON(yyjson_val *obj) {
	UpgradeFormatVersionUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

UpgradeFormatVersionUpdate UpgradeFormatVersionUpdate::Copy() const {
	UpgradeFormatVersionUpdate res;
	res.base_update = base_update.Copy();
	res.format_version = format_version;
	return res;
}

string UpgradeFormatVersionUpdate::TryFromJSON(yyjson_val *obj) {
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
			    "UpgradeFormatVersionUpdate property 'action_refinement' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_refinement_val));
		}
		if (!yyjson_is_null(action_refinement_val) && action_refinement != "upgrade-format-version") {
			return "UpgradeFormatVersionUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "UpgradeFormatVersionUpdate required property 'action' is missing";
	}
	auto format_version_val = yyjson_obj_get(obj, "format-version");
	if (!format_version_val) {
		return "UpgradeFormatVersionUpdate required property 'format-version' is missing";
	} else {
		if (yyjson_is_int(format_version_val)) {
			format_version = yyjson_get_int(format_version_val);
		} else {
			return StringUtil::Format(
			    "UpgradeFormatVersionUpdate property 'format_version' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(format_version_val));
		}
	}
	return "";
}

void UpgradeFormatVersionUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: format-version
	yyjson_mut_obj_add_int(doc, obj, "format-version", format_version);
}

yyjson_mut_val *UpgradeFormatVersionUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
