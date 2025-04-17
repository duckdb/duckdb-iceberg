
#include "rest_catalog/objects/upgrade_format_version_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
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

string UpgradeFormatVersionUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
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
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format(
			    "UpgradeFormatVersionUpdate property 'action' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
