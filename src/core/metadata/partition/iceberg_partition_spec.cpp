#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "catalog/rest/api/catalog_utils.hpp"

namespace duckdb {

IcebergPartitionSpecField IcebergPartitionSpecField::ParseFromJson(const rest_api_objects::PartitionField &field,
                                                                   optional_idx assigned_field_id) {
	IcebergPartitionSpecField result;

	result.name = field.name;
	result.transform = field.transform.value;
	result.source_id = field.source_id;
	if (assigned_field_id.IsValid()) {
		result.partition_field_id = assigned_field_id.GetIndex();
	} else {
		if (!field.field_id) {
			throw InvalidConfigurationException("Partition field '%s' is missing 'field-id'", field.name);
		}
		result.partition_field_id = *field.field_id;
	}
	return result;
}

bool IcebergPartitionSpecField::Equals(const IcebergPartitionSpecField &other) const {
	return source_id == other.source_id && transform.RawType() == other.transform.RawType();
}

bool IcebergPartitionSpec::Equals(const IcebergPartitionSpec &other) const {
	if (fields.size() != other.fields.size()) {
		return false;
	}
	for (idx_t i = 0; i < fields.size(); i++) {
		if (!fields[i].Equals(other.fields[i]) ||
		    fields[i].GetPartitionSpecFieldName() != other.fields[i].GetPartitionSpecFieldName()) {
			return false;
		}
	}
	return true;
}

IcebergPartitionSpec IcebergPartitionSpec::ParseFromJson(const rest_api_objects::PartitionSpec &partition_spec,
                                                         int32_t iceberg_version) {
	D_ASSERT(partition_spec.spec_id);
	IcebergPartitionSpec result(*partition_spec.spec_id);
	auto &fields = partition_spec.fields;
	idx_t missing_field_ids = 0;
	for (auto &field : fields) {
		if (!field.field_id) {
			missing_field_ids++;
		}
	}
	//! v1 partition field ids are optional and default to 1000 + position in the spec
	bool assign_field_ids = iceberg_version == 1 && missing_field_ids > 0;
	if (assign_field_ids && missing_field_ids != fields.size()) {
		// ids assigned by position could collide with the ones that are set
		throw InvalidConfigurationException(
		    "Cannot parse partition spec %d with missing field IDs: %d missing of %d fields", result.spec_id,
		    missing_field_ids, fields.size());
	}
	for (idx_t i = 0; i < fields.size(); i++) {
		optional_idx assigned_field_id;
		if (assign_field_ids) {
			assigned_field_id = 1000 + i;
		}
		result.fields.push_back(IcebergPartitionSpecField::ParseFromJson(fields[i], assigned_field_id));
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

JSONMutableValue IcebergPartitionSpec::FieldsToJSON(JSONWriter &writer) const {
	auto fields_array = writer.CreateArray();
	for (auto &field : fields) {
		auto field_obj = writer.CreateObject();
		fields_array.Append(field_obj);
		field_obj.AddString("name", field.GetPartitionSpecFieldName());
		field_obj.AddString("transform", field.transform.RawType());
		field_obj.Add("source-id", writer.CreateUnsignedInteger(field.source_id));
		field_obj.Add("field-id", writer.CreateUnsignedInteger(field.partition_field_id));
	}
	return fields_array;
}

string IcebergPartitionSpec::FieldsToJSONString() const {
	JSONWriter writer;
	writer.SetRoot(FieldsToJSON(writer));
	return writer.ToString(JSONWriteFlags::ALLOW_INF_AND_NAN);
}

JSONMutableValue IcebergPartitionSpec::ToJSON(JSONWriter &writer) const {
	auto partition_obj = writer.CreateObject();
	partition_obj.Add("spec-id", writer.CreateSignedInteger(spec_id));
	partition_obj.Add("fields", FieldsToJSON(writer));
	return partition_obj;
}

} // namespace duckdb
