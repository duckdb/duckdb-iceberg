
#include "rest_catalog/objects/table_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

TableUpdate TableUpdate::Copy() const {
	TableUpdate res;
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
	if (has_set_current_schema_update) {
		res.set_current_schema_update = set_current_schema_update.Copy();
	}
	res.has_set_current_schema_update = has_set_current_schema_update;
	if (has_add_partition_spec_update) {
		res.add_partition_spec_update = add_partition_spec_update.Copy();
	}
	res.has_add_partition_spec_update = has_add_partition_spec_update;
	if (has_set_default_spec_update) {
		res.set_default_spec_update = set_default_spec_update.Copy();
	}
	res.has_set_default_spec_update = has_set_default_spec_update;
	if (has_add_sort_order_update) {
		res.add_sort_order_update = add_sort_order_update.Copy();
	}
	res.has_add_sort_order_update = has_add_sort_order_update;
	if (has_set_default_sort_order_update) {
		res.set_default_sort_order_update = set_default_sort_order_update.Copy();
	}
	res.has_set_default_sort_order_update = has_set_default_sort_order_update;
	if (has_add_snapshot_update) {
		res.add_snapshot_update = add_snapshot_update.Copy();
	}
	res.has_add_snapshot_update = has_add_snapshot_update;
	if (has_set_snapshot_ref_update) {
		res.set_snapshot_ref_update = set_snapshot_ref_update.Copy();
	}
	res.has_set_snapshot_ref_update = has_set_snapshot_ref_update;
	if (has_remove_snapshots_update) {
		res.remove_snapshots_update = remove_snapshots_update.Copy();
	}
	res.has_remove_snapshots_update = has_remove_snapshots_update;
	if (has_remove_snapshot_ref_update) {
		res.remove_snapshot_ref_update = remove_snapshot_ref_update.Copy();
	}
	res.has_remove_snapshot_ref_update = has_remove_snapshot_ref_update;
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
	if (has_set_statistics_update) {
		res.set_statistics_update = set_statistics_update.Copy();
	}
	res.has_set_statistics_update = has_set_statistics_update;
	if (has_remove_statistics_update) {
		res.remove_statistics_update = remove_statistics_update.Copy();
	}
	res.has_remove_statistics_update = has_remove_statistics_update;
	if (has_remove_partition_specs_update) {
		res.remove_partition_specs_update = remove_partition_specs_update.Copy();
	}
	res.has_remove_partition_specs_update = has_remove_partition_specs_update;
	if (has_remove_schemas_update) {
		res.remove_schemas_update = remove_schemas_update.Copy();
	}
	res.has_remove_schemas_update = has_remove_schemas_update;
	if (has_add_encryption_key_update) {
		res.add_encryption_key_update = add_encryption_key_update.Copy();
	}
	res.has_add_encryption_key_update = has_add_encryption_key_update;
	if (has_remove_encryption_key_update) {
		res.remove_encryption_key_update = remove_encryption_key_update.Copy();
	}
	res.has_remove_encryption_key_update = has_remove_encryption_key_update;
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
	error = add_encryption_key_update.TryFromJSON(obj);
	if (error.empty()) {
		has_add_encryption_key_update = true;
	}
	error = remove_encryption_key_update.TryFromJSON(obj);
	if (error.empty()) {
		has_remove_encryption_key_update = true;
	}
	if (!has_add_encryption_key_update && !has_add_partition_spec_update && !has_add_schema_update &&
	    !has_add_snapshot_update && !has_add_sort_order_update && !has_assign_uuidupdate &&
	    !has_remove_encryption_key_update && !has_remove_partition_specs_update && !has_remove_properties_update &&
	    !has_remove_schemas_update && !has_remove_snapshot_ref_update && !has_remove_snapshots_update &&
	    !has_remove_statistics_update && !has_set_current_schema_update && !has_set_default_sort_order_update &&
	    !has_set_default_spec_update && !has_set_location_update && !has_set_properties_update &&
	    !has_set_snapshot_ref_update && !has_set_statistics_update && !has_upgrade_format_version_update) {
		return "TableUpdate failed to parse, none of the anyOf candidates matched";
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
