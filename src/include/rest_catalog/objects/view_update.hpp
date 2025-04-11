
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/add_schema_update.hpp"
#include "rest_catalog/objects/add_view_version_update.hpp"
#include "rest_catalog/objects/assign_uuidupdate.hpp"
#include "rest_catalog/objects/remove_properties_update.hpp"
#include "rest_catalog/objects/set_current_view_version_update.hpp"
#include "rest_catalog/objects/set_location_update.hpp"
#include "rest_catalog/objects/set_properties_update.hpp"
#include "rest_catalog/objects/upgrade_format_version_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewUpdate {
public:
	ViewUpdate();
	ViewUpdate(const ViewUpdate &) = delete;
	ViewUpdate &operator=(const ViewUpdate &) = delete;
	ViewUpdate(ViewUpdate &&) = default;
	ViewUpdate &operator=(ViewUpdate &&) = default;

public:
	static ViewUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	AssignUUIDUpdate assign_uuidupdate;
	bool has_assign_uuidupdate = false;
	UpgradeFormatVersionUpdate upgrade_format_version_update;
	bool has_upgrade_format_version_update = false;
	AddSchemaUpdate add_schema_update;
	bool has_add_schema_update = false;
	SetLocationUpdate set_location_update;
	bool has_set_location_update = false;
	SetPropertiesUpdate set_properties_update;
	bool has_set_properties_update = false;
	RemovePropertiesUpdate remove_properties_update;
	bool has_remove_properties_update = false;
	AddViewVersionUpdate add_view_version_update;
	bool has_add_view_version_update = false;
	SetCurrentViewVersionUpdate set_current_view_version_update;
	bool has_set_current_view_version_update = false;
};

} // namespace rest_api_objects
} // namespace duckdb
