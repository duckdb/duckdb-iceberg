
#include "rest_catalog/objects/view_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ViewUpdate::ViewUpdate() {
}

ViewUpdate ViewUpdate::FromJSON(JSONValue obj) {
	ViewUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ViewUpdate ViewUpdate::Copy() const {
	ViewUpdate res;
	if (assign_uuidupdate.has_value()) {
		res.assign_uuidupdate.emplace();
		(*res.assign_uuidupdate) = (*assign_uuidupdate).Copy();
	}
	if (upgrade_format_version_update.has_value()) {
		res.upgrade_format_version_update.emplace();
		(*res.upgrade_format_version_update) = (*upgrade_format_version_update).Copy();
	}
	if (add_schema_update.has_value()) {
		res.add_schema_update.emplace();
		(*res.add_schema_update) = (*add_schema_update).Copy();
	}
	if (set_location_update.has_value()) {
		res.set_location_update.emplace();
		(*res.set_location_update) = (*set_location_update).Copy();
	}
	if (set_properties_update.has_value()) {
		res.set_properties_update.emplace();
		(*res.set_properties_update) = (*set_properties_update).Copy();
	}
	if (remove_properties_update.has_value()) {
		res.remove_properties_update.emplace();
		(*res.remove_properties_update) = (*remove_properties_update).Copy();
	}
	if (add_view_version_update.has_value()) {
		res.add_view_version_update.emplace();
		(*res.add_view_version_update) = (*add_view_version_update).Copy();
	}
	if (set_current_view_version_update.has_value()) {
		res.set_current_view_version_update.emplace();
		(*res.set_current_view_version_update) = (*set_current_view_version_update).Copy();
	}
	return res;
}

string ViewUpdate::TryFromJSON(JSONValue obj) {
	string error;
	assign_uuidupdate.emplace();
	error = assign_uuidupdate->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		assign_uuidupdate = nullopt;
	}
	upgrade_format_version_update.emplace();
	error = upgrade_format_version_update->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		upgrade_format_version_update = nullopt;
	}
	add_schema_update.emplace();
	error = add_schema_update->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		add_schema_update = nullopt;
	}
	set_location_update.emplace();
	error = set_location_update->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		set_location_update = nullopt;
	}
	set_properties_update.emplace();
	error = set_properties_update->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		set_properties_update = nullopt;
	}
	remove_properties_update.emplace();
	error = remove_properties_update->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		remove_properties_update = nullopt;
	}
	add_view_version_update.emplace();
	error = add_view_version_update->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		add_view_version_update = nullopt;
	}
	set_current_view_version_update.emplace();
	error = set_current_view_version_update->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		set_current_view_version_update = nullopt;
	}
	if (!(add_schema_update.has_value()) && !(add_view_version_update.has_value()) &&
	    !(assign_uuidupdate.has_value()) && !(remove_properties_update.has_value()) &&
	    !(set_current_view_version_update.has_value()) && !(set_location_update.has_value()) &&
	    !(set_properties_update.has_value()) && !(upgrade_format_version_update.has_value())) {
		return "ViewUpdate failed to parse, none of the anyOf candidates matched";
	}
	return "";
}

void ViewUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	if (assign_uuidupdate.has_value()) {
		assign_uuidupdate->PopulateJSON(writer, obj);
	} else if (upgrade_format_version_update.has_value()) {
		upgrade_format_version_update->PopulateJSON(writer, obj);
	} else if (add_schema_update.has_value()) {
		add_schema_update->PopulateJSON(writer, obj);
	} else if (set_location_update.has_value()) {
		set_location_update->PopulateJSON(writer, obj);
	} else if (set_properties_update.has_value()) {
		set_properties_update->PopulateJSON(writer, obj);
	} else if (remove_properties_update.has_value()) {
		remove_properties_update->PopulateJSON(writer, obj);
	} else if (add_view_version_update.has_value()) {
		add_view_version_update->PopulateJSON(writer, obj);
	} else if (set_current_view_version_update.has_value()) {
		set_current_view_version_update->PopulateJSON(writer, obj);
	}
}

JSONMutableValue ViewUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
