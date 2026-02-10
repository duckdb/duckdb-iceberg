#include "metadata/iceberg_partition_spec.hpp"
#include "catalog_utils.hpp"

namespace duckdb {

IcebergPartitionSpecField IcebergPartitionSpecField::ParseFromJson(const rest_api_objects::PartitionField &field) {
	IcebergPartitionSpecField result;

	result.name = field.name;
	result.transform = field.transform.value;
	result.source_id = field.source_id;
	D_ASSERT(field.has_field_id);
	result.partition_field_id = field.field_id;
	return result;
}

IcebergPartitionSpec IcebergPartitionSpec::ParseFromJson(const rest_api_objects::PartitionSpec &partition_spec) {
	IcebergPartitionSpec result;

	D_ASSERT(partition_spec.has_spec_id);
	result.spec_id = partition_spec.spec_id;
	for (auto &field : partition_spec.fields) {
		result.fields.push_back(IcebergPartitionSpecField::ParseFromJson(field));
	}
	return result;
}

bool IcebergPartitionSpec::IsPartitioned() const {
	//! A partition spec is considered partitioned if it has at least one field that doesn't have a 'void' transform
	for (const auto &field : fields) {
		if (field.transform != IcebergTransformType::VOID) {
			return true;
		}
	}

	return false;
}

bool IcebergPartitionSpec::IsUnpartitioned() const {
	return !IsPartitioned();
}

const vector<IcebergPartitionSpecField> &IcebergPartitionSpec::GetFields() const {
	return fields;
}

const IcebergPartitionSpecField &IcebergPartitionSpec::GetFieldBySourceId(idx_t source_id) const {
	for (auto &field : fields) {
		if (field.source_id == source_id) {
			return field;
		}
	}
	throw InvalidConfigurationException("Field with source_id %d doesn't exist in this partition spec (id %d)",
	                                    source_id, spec_id);
}

string IcebergPartitionSpec::FieldsToJSON() const {
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	auto doc = doc_p.get();
	auto root_arr = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, root_arr);

	for (auto &field : fields) {
		auto field_obj = yyjson_mut_arr_add_obj(doc, root_arr);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", field.name.c_str());
		yyjson_mut_obj_add_strcpy(doc, field_obj, "transform", field.transform.RawType().c_str());
		yyjson_mut_obj_add_uint(doc, field_obj, "source-id", field.source_id);
		yyjson_mut_obj_add_uint(doc, field_obj, "field-id", field.partition_field_id);
	}

	return ICUtils::JsonToString(std::move(doc_p));
}

} // namespace duckdb
