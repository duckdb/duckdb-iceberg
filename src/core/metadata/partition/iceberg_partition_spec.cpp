#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "catalog/rest/api/catalog_utils.hpp"

namespace duckdb {

IcebergPartitionSpecField IcebergPartitionSpecField::ParseFromJson(const rest_api_objects::PartitionField &field) {
	IcebergPartitionSpecField result;

	result.name = field.name;
	result.transform = field.transform.value;
	result.source_id = field.source_id;
	D_ASSERT(field.field_id);
	result.partition_field_id = *field.field_id;
	return result;
}

bool IcebergPartitionSpec::Equals(const IcebergPartitionSpec &other) const {
	if (other.fields.size() != fields.size()) {
		return false;
	}
	for (idx_t i = 0; i < other.fields.size(); i++) {
		auto existing_partition_col_source_id = other.fields[i].source_id;

		//! Compare source ids
		auto new_spec_col_source_id = fields[i].source_id;
		if (existing_partition_col_source_id != new_spec_col_source_id) {
			return false;
		}

		//! Compare transforms
		auto existing_partition_col_transform = other.fields[i].transform.RawType();
		auto new_spec_col_transform = fields[i].transform.RawType();
		if (existing_partition_col_transform != new_spec_col_transform) {
			return false;
		}
	}
	return true;
}

IcebergPartitionSpec IcebergPartitionSpec::ParseFromJson(const rest_api_objects::PartitionSpec &partition_spec) {
	D_ASSERT(partition_spec.spec_id);
	IcebergPartitionSpec result(*partition_spec.spec_id);
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

void IcebergPartitionSpecField::SetPartitionSpecFieldName(const string &column_name) {
	string transform_raw_type = transform.RawType();
	for (idx_t i = 0; i < transform_raw_type.size(); i++) {
		char c = transform_raw_type[i];
		bool valid = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
		if (!valid) {
			transform_raw_type[i] = '_';
		}
	}
	// Avro names must not start with a digit
	if (!transform_raw_type.empty() && transform_raw_type[0] >= '0' && transform_raw_type[0] <= '9') {
		transform_raw_type = "_" + transform_raw_type;
	}
	name = transform_raw_type + "_" + column_name + "_" + to_string(source_id);
}

const string &IcebergPartitionSpecField::GetPartitionSpecFieldName() const {
	return name;
}

optional_ptr<const IcebergPartitionSpecField> IcebergPartitionSpec::TryGetFieldBySourceId(idx_t source_id) const {
	for (auto &field : fields) {
		if (field.source_id == source_id) {
			return field;
		}
	}
	return nullptr;
}

const IcebergPartitionSpecField &IcebergPartitionSpec::GetFieldBySourceId(idx_t source_id) const {
	auto res = TryGetFieldBySourceId(source_id);
	if (!res) {
		throw InvalidConfigurationException("Field with source_id %d doesn't exist in this partition spec (id %d)",
		                                    source_id, spec_id);
	}
	return *res;
}

yyjson_mut_val *IcebergPartitionSpec::FieldsToJSON(yyjson_mut_doc *doc) const {
	auto fields_array = yyjson_mut_arr(doc);
	for (auto &field : fields) {
		auto field_obj = yyjson_mut_arr_add_obj(doc, fields_array);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", field.GetPartitionSpecFieldName().c_str());
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

} // namespace duckdb
