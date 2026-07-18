
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
	if (assert_create.has_value()) {
		res.assert_create.emplace();
		(*res.assert_create) = (*assert_create).Copy();
	}
	if (assert_table_uuid.has_value()) {
		res.assert_table_uuid.emplace();
		(*res.assert_table_uuid) = (*assert_table_uuid).Copy();
	}
	if (assert_ref_snapshot_id.has_value()) {
		res.assert_ref_snapshot_id.emplace();
		(*res.assert_ref_snapshot_id) = (*assert_ref_snapshot_id).Copy();
	}
	if (assert_last_assigned_field_id.has_value()) {
		res.assert_last_assigned_field_id.emplace();
		(*res.assert_last_assigned_field_id) = (*assert_last_assigned_field_id).Copy();
	}
	if (assert_current_schema_id.has_value()) {
		res.assert_current_schema_id.emplace();
		(*res.assert_current_schema_id) = (*assert_current_schema_id).Copy();
	}
	if (assert_last_assigned_partition_id.has_value()) {
		res.assert_last_assigned_partition_id.emplace();
		(*res.assert_last_assigned_partition_id) = (*assert_last_assigned_partition_id).Copy();
	}
	if (assert_default_spec_id.has_value()) {
		res.assert_default_spec_id.emplace();
		(*res.assert_default_spec_id) = (*assert_default_spec_id).Copy();
	}
	if (assert_default_sort_order_id.has_value()) {
		res.assert_default_sort_order_id.emplace();
		(*res.assert_default_sort_order_id) = (*assert_default_sort_order_id).Copy();
	}
	return res;
}

string TableRequirement::TryFromJSON(yyjson_val *obj) {
	string error;
	auto discriminator_val = yyjson_obj_get(obj, "type");
	if (!discriminator_val || !yyjson_is_str(discriminator_val)) {
		return "TableRequirement discriminator 'type' is missing or is not a string";
	}
	string discriminator = yyjson_get_str(discriminator_val);
	if (discriminator == "assert-create") {
		assert_create.emplace();
		error = assert_create->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "assert-table-uuid") {
		assert_table_uuid.emplace();
		error = assert_table_uuid->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "assert-ref-snapshot-id") {
		assert_ref_snapshot_id.emplace();
		error = assert_ref_snapshot_id->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "assert-last-assigned-field-id") {
		assert_last_assigned_field_id.emplace();
		error = assert_last_assigned_field_id->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "assert-current-schema-id") {
		assert_current_schema_id.emplace();
		error = assert_current_schema_id->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "assert-last-assigned-partition-id") {
		assert_last_assigned_partition_id.emplace();
		error = assert_last_assigned_partition_id->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "assert-default-spec-id") {
		assert_default_spec_id.emplace();
		error = assert_default_spec_id->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "assert-default-sort-order-id") {
		assert_default_sort_order_id.emplace();
		error = assert_default_sort_order_id->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else {
		return StringUtil::Format("TableRequirement has unknown discriminator value '%s'", discriminator.c_str());
	}
	return "";
}

void TableRequirement::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	if (assert_create.has_value()) {
		assert_create->PopulateJSON(doc, obj);
	} else if (assert_table_uuid.has_value()) {
		assert_table_uuid->PopulateJSON(doc, obj);
	} else if (assert_ref_snapshot_id.has_value()) {
		assert_ref_snapshot_id->PopulateJSON(doc, obj);
	} else if (assert_last_assigned_field_id.has_value()) {
		assert_last_assigned_field_id->PopulateJSON(doc, obj);
	} else if (assert_current_schema_id.has_value()) {
		assert_current_schema_id->PopulateJSON(doc, obj);
	} else if (assert_last_assigned_partition_id.has_value()) {
		assert_last_assigned_partition_id->PopulateJSON(doc, obj);
	} else if (assert_default_spec_id.has_value()) {
		assert_default_spec_id->PopulateJSON(doc, obj);
	} else if (assert_default_sort_order_id.has_value()) {
		assert_default_sort_order_id->PopulateJSON(doc, obj);
	}
}

yyjson_mut_val *TableRequirement::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
