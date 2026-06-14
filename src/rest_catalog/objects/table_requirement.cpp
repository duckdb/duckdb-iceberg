
#include "rest_catalog/objects/table_requirement.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

TableRequirement::TableRequirement() {
}

TableRequirement TableRequirement::FromJSON(yyjson_val *obj) {
	TableRequirement res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

TableRequirement TableRequirement::Copy() const {
	TableRequirement res;
	if (has_assert_create) {
		res.assert_create = assert_create.Copy();
	}
	res.has_assert_create = has_assert_create;
	if (has_assert_table_uuid) {
		res.assert_table_uuid = assert_table_uuid.Copy();
	}
	res.has_assert_table_uuid = has_assert_table_uuid;
	if (has_assert_ref_snapshot_id) {
		res.assert_ref_snapshot_id = assert_ref_snapshot_id.Copy();
	}
	res.has_assert_ref_snapshot_id = has_assert_ref_snapshot_id;
	if (has_assert_last_assigned_field_id) {
		res.assert_last_assigned_field_id = assert_last_assigned_field_id.Copy();
	}
	res.has_assert_last_assigned_field_id = has_assert_last_assigned_field_id;
	if (has_assert_current_schema_id) {
		res.assert_current_schema_id = assert_current_schema_id.Copy();
	}
	res.has_assert_current_schema_id = has_assert_current_schema_id;
	if (has_assert_last_assigned_partition_id) {
		res.assert_last_assigned_partition_id = assert_last_assigned_partition_id.Copy();
	}
	res.has_assert_last_assigned_partition_id = has_assert_last_assigned_partition_id;
	if (has_assert_default_spec_id) {
		res.assert_default_spec_id = assert_default_spec_id.Copy();
	}
	res.has_assert_default_spec_id = has_assert_default_spec_id;
	if (has_assert_default_sort_order_id) {
		res.assert_default_sort_order_id = assert_default_sort_order_id.Copy();
	}
	res.has_assert_default_sort_order_id = has_assert_default_sort_order_id;
	return res;
}
string TableRequirement::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = assert_create.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_create = true;
			break;
		}
		error = assert_table_uuid.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_table_uuid = true;
			break;
		}
		error = assert_ref_snapshot_id.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_ref_snapshot_id = true;
			break;
		}
		error = assert_last_assigned_field_id.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_last_assigned_field_id = true;
			break;
		}
		error = assert_current_schema_id.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_current_schema_id = true;
			break;
		}
		error = assert_last_assigned_partition_id.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_last_assigned_partition_id = true;
			break;
		}
		error = assert_default_spec_id.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_default_spec_id = true;
			break;
		}
		error = assert_default_sort_order_id.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_default_sort_order_id = true;
			break;
		}
		return "TableRequirement failed to parse, none of the oneOf candidates matched";
	} while (false);
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
