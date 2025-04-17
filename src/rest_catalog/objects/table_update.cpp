
#include "rest_catalog/objects/table_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

TableUpdate::TableUpdate() {
}

TableUpdate TableUpdate::FromJSON(yyjson_val *obj) {
	TableUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string TableUpdate::TryFromJSON(yyjson_val *obj) {
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
	error = set_current_schema_update.TryFromJSON(obj);
	if (error.empty()) {
		has_set_current_schema_update = true;
	}
	error = add_partition_spec_update.TryFromJSON(obj);
	if (error.empty()) {
		has_add_partition_spec_update = true;
	}
	error = set_default_spec_update.TryFromJSON(obj);
	if (error.empty()) {
		has_set_default_spec_update = true;
	}
	error = add_sort_order_update.TryFromJSON(obj);
	if (error.empty()) {
		has_add_sort_order_update = true;
	}
	error = set_default_sort_order_update.TryFromJSON(obj);
	if (error.empty()) {
		has_set_default_sort_order_update = true;
	}
	error = add_snapshot_update.TryFromJSON(obj);
	if (error.empty()) {
		has_add_snapshot_update = true;
	}
	error = set_snapshot_ref_update.TryFromJSON(obj);
	if (error.empty()) {
		has_set_snapshot_ref_update = true;
	}
	error = remove_snapshots_update.TryFromJSON(obj);
	if (error.empty()) {
		has_remove_snapshots_update = true;
	}
	error = remove_snapshot_ref_update.TryFromJSON(obj);
	if (error.empty()) {
		has_remove_snapshot_ref_update = true;
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
	error = set_statistics_update.TryFromJSON(obj);
	if (error.empty()) {
		has_set_statistics_update = true;
	}
	error = remove_statistics_update.TryFromJSON(obj);
	if (error.empty()) {
		has_remove_statistics_update = true;
	}
	error = remove_partition_specs_update.TryFromJSON(obj);
	if (error.empty()) {
		has_remove_partition_specs_update = true;
	}
	error = remove_schemas_update.TryFromJSON(obj);
	if (error.empty()) {
		has_remove_schemas_update = true;
	}
	error = enable_row_lineage_update.TryFromJSON(obj);
	if (error.empty()) {
		has_enable_row_lineage_update = true;
	}
	if (!has_add_partition_spec_update && !has_add_schema_update && !has_add_snapshot_update &&
	    !has_add_sort_order_update && !has_assign_uuidupdate && !has_enable_row_lineage_update &&
	    !has_remove_partition_specs_update && !has_remove_properties_update && !has_remove_schemas_update &&
	    !has_remove_snapshot_ref_update && !has_remove_snapshots_update && !has_remove_statistics_update &&
	    !has_set_current_schema_update && !has_set_default_sort_order_update && !has_set_default_spec_update &&
	    !has_set_location_update && !has_set_properties_update && !has_set_snapshot_ref_update &&
	    !has_set_statistics_update && !has_upgrade_format_version_update) {
		return "TableUpdate failed to parse, none of the anyOf candidates matched";
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
