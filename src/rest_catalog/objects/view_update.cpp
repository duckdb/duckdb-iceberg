
#include "rest_catalog/objects/view_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ViewUpdate::ViewUpdate() {
}

ViewUpdate ViewUpdate::FromJSON(yyjson_val *obj) {
	ViewUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ViewUpdate ViewUpdate::Copy() const {
	ViewUpdate res;
	if (has_assign_uuidupdate) {
		res.assign_uuidupdate = assign_uuidupdate.Copy();
	}
	res.has_assign_uuidupdate = has_assign_uuidupdate;
	if (has_upgrade_format_version_update) {
		res.upgrade_format_version_update = upgrade_format_version_update.Copy();
	}
	res.has_upgrade_format_version_update = has_upgrade_format_version_update;
	if (has_add_schema_update) {
		res.add_schema_update = add_schema_update.Copy();
	}
	res.has_add_schema_update = has_add_schema_update;
	if (has_set_location_update) {
		res.set_location_update = set_location_update.Copy();
	}
	res.has_set_location_update = has_set_location_update;
	if (has_set_properties_update) {
		res.set_properties_update = set_properties_update.Copy();
	}
	res.has_set_properties_update = has_set_properties_update;
	if (has_remove_properties_update) {
		res.remove_properties_update = remove_properties_update.Copy();
	}
	res.has_remove_properties_update = has_remove_properties_update;
	if (has_add_view_version_update) {
		res.add_view_version_update = add_view_version_update.Copy();
	}
	res.has_add_view_version_update = has_add_view_version_update;
	if (has_set_current_view_version_update) {
		res.set_current_view_version_update = set_current_view_version_update.Copy();
	}
	res.has_set_current_view_version_update = has_set_current_view_version_update;
	return res;
}
string ViewUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = assign_uuidupdate.TryFromJSON(obj);
	if (error.empty()) {
		has_assign_uuidupdate = true;
	}
	error = upgrade_format_version_update.TryFromJSON(obj);
	if (error.empty()) {
		has_upgrade_format_version_update = true;
	}
	error = add_schema_update.TryFromJSON(obj);
	if (error.empty()) {
		has_add_schema_update = true;
	}
	error = set_location_update.TryFromJSON(obj);
	if (error.empty()) {
		has_set_location_update = true;
	}
	error = set_properties_update.TryFromJSON(obj);
	if (error.empty()) {
		has_set_properties_update = true;
	}
	error = remove_properties_update.TryFromJSON(obj);
	if (error.empty()) {
		has_remove_properties_update = true;
	}
	error = add_view_version_update.TryFromJSON(obj);
	if (error.empty()) {
		has_add_view_version_update = true;
	}
	error = set_current_view_version_update.TryFromJSON(obj);
	if (error.empty()) {
		has_set_current_view_version_update = true;
	}
	if (!has_add_schema_update && !has_add_view_version_update && !has_assign_uuidupdate &&
	    !has_remove_properties_update && !has_set_current_view_version_update && !has_set_location_update &&
	    !has_set_properties_update && !has_upgrade_format_version_update) {
		return "ViewUpdate failed to parse, none of the anyOf candidates matched";
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
