#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "catalog/rest/api/catalog_utils.hpp"

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
	D_ASSERT(partition_spec.has_spec_id);
	IcebergPartitionSpec result(partition_spec.spec_id);
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

yyjson_mut_val *IcebergPartitionSpec::FieldsToJSON(yyjson_mut_doc *doc) const {
	auto fields_array = yyjson_mut_arr(doc);
	for (auto &field : fields) {
		auto field_obj = yyjson_mut_arr_add_obj(doc, fields_array);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", field.name.c_str());
		yyjson_mut_obj_add_strcpy(doc, field_obj, "transform", field.transform.RawType().c_str());
		yyjson_mut_obj_add_uint(doc, field_obj, "source-id", field.source_id);
		yyjson_mut_obj_add_uint(doc, field_obj, "field-id", field.partition_field_id);
	}
	return fields_array;
}

string IcebergPartitionSpec::FieldsToJSONString() const {
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	auto doc = doc_p.get();
	auto root_arr = FieldsToJSON(doc);
	yyjson_mut_doc_set_root(doc, root_arr);
	return ICUtils::JsonToString(std::move(doc_p));
}

yyjson_mut_val *IcebergPartitionSpec::ToJSON(yyjson_mut_doc *doc) const {
	auto partition_obj = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, partition_obj, "spec-id", yyjson_mut_int(doc, spec_id));
	yyjson_mut_obj_add_val(doc, partition_obj, "fields", FieldsToJSON(doc));
	return partition_obj;
}

yyjson_mut_val *IcebergPartitionSpec::ToJSON(yyjson_mut_doc *doc, const rest_api_objects::PartitionSpec &spec) {
	auto res = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_int(doc, res, "spec-id", spec.spec_id);
	auto &fields = spec.fields;
	auto fields_array = yyjson_mut_arr(doc);

	for (auto &field : fields) {
		auto field_obj = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", field.name.c_str());
		yyjson_mut_obj_add_strcpy(doc, field_obj, "transform", field.transform.value.c_str());
		yyjson_mut_obj_add_int(doc, field_obj, "source-id", field.source_id);
		yyjson_mut_obj_add_int(doc, field_obj, "field-id", field.field_id);
		yyjson_mut_arr_add_val(fields_array, field_obj);
	}
	yyjson_mut_obj_add_val(doc, res, "fields", fields_array);
	return res;
}

} // namespace duckdb
