#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

IcebergPartitionSpecField IcebergPartitionSpecField::ParseFromJson(const rest_api_objects::PartitionField &field,
                                                                   optional_idx assigned_field_id) {
	IcebergPartitionSpecField result;

	result.name = field.name;
	result.transform = field.transform.value;
	if (field.source_id <= 0) {
		throw InvalidConfigurationException("Invalid source id %d for partition field '%s'", field.source_id,
		                                    field.name);
	}
	result.source_id = field.source_id;
	if (assigned_field_id.IsValid()) {
		result.partition_field_id = assigned_field_id.GetIndex();
	} else {
		if (!field.field_id) {
			throw InvalidConfigurationException("Partition field '%s' is missing 'field-id'", field.name);
		}
		if (*field.field_id <= 0) {
			throw InvalidConfigurationException("Invalid partition field id %d for '%s'", *field.field_id, field.name);
		}
		result.partition_field_id = *field.field_id;
	}
	return result;
}

bool IcebergPartitionSpecField::Equals(const IcebergPartitionSpecField &other) const {
	return source_id == other.source_id && transform.RawType() == other.transform.RawType();
}

bool IcebergPartitionSpec::Equals(const IcebergPartitionSpec &other, bool compare_field_ids) const {
	if (fields.size() != other.fields.size()) {
		return false;
	}
	for (idx_t i = 0; i < fields.size(); i++) {
		if (!fields[i].Equals(other.fields[i]) ||
		    fields[i].GetPartitionSpecFieldName() != other.fields[i].GetPartitionSpecFieldName()) {
			return false;
		}
		if (compare_field_ids && fields[i].partition_field_id != other.fields[i].partition_field_id) {
			return false;
		}
	}
	return true;
}

IcebergPartitionSpec IcebergPartitionSpec::ParseFromJson(const rest_api_objects::PartitionSpec &partition_spec,
                                                         int32_t iceberg_version) {
	if (!partition_spec.spec_id || *partition_spec.spec_id < 0) {
		throw InvalidConfigurationException("Iceberg partition spec must have a non-negative 'spec-id'");
	}
	return FromFields(partition_spec.fields, *partition_spec.spec_id, iceberg_version);
}

IcebergPartitionSpec IcebergPartitionSpec::FromFields(const vector<rest_api_objects::PartitionField> &fields,
                                                      int32_t spec_id, int32_t iceberg_version) {
	IcebergPartitionSpec result(spec_id);
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
	unordered_set<idx_t> field_ids;
	for (idx_t i = 0; i < fields.size(); i++) {
		optional_idx assigned_field_id;
		if (assign_field_ids) {
			assigned_field_id = 1000 + i;
		}
		auto field = IcebergPartitionSpecField::ParseFromJson(fields[i], assigned_field_id);
		if (!field_ids.insert(field.partition_field_id).second) {
			throw InvalidConfigurationException("Duplicate partition field id %d in spec %d", field.partition_field_id,
			                                    spec_id);
		}
		result.fields.push_back(std::move(field));
	}
	return result;
}

bool IcebergPartitionSpec::IsPartitioned() const {
	//! A partition spec is considered partitioned if it has at least one field that doesn't have a 'void' transform
	for (const auto &field : fields) {
		if (field.transform != IcebergTransformType::VOID_TRANSFORM) {
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

void IcebergPartitionSpecField::SetPartitionSpecFieldName(const string &column_name, const string &suffix) {
	switch (transform.Type()) {
	case IcebergTransformType::IDENTITY:
		name = column_name;
		break;
	case IcebergTransformType::BUCKET:
		name = StringUtil::Format("%s_bucket_%d", column_name, transform.GetBucketModulo());
		break;
	case IcebergTransformType::TRUNCATE:
		name = StringUtil::Format("%s_trunc_%d", column_name, transform.GetTruncateWidth());
		break;
	case IcebergTransformType::YEAR:
		name = column_name + "_year";
		break;
	case IcebergTransformType::MONTH:
		name = column_name + "_month";
		break;
	case IcebergTransformType::DAY:
		name = column_name + "_day";
		break;
	case IcebergTransformType::HOUR:
		name = column_name + "_hour";
		break;
	default:
		throw NotImplementedException("No default partition field name for transform '%s'", transform.RawType());
	}
	name += suffix;
}

void IcebergPartitionSpec::FinalizeFieldNames(const IcebergTableSchema &schema, idx_t first_new_field_id) {
	case_insensitive_map_t<int32_t> schema_field_ids;
	case_insensitive_set_t reserved_names;
	for (auto &field : schema.GetFullNames()) {
		schema_field_ids.emplace(field.second, field.first);
		reserved_names.insert(field.second);
	}

	case_insensitive_set_t assigned_names;
	unordered_map<uint64_t, unordered_set<string>> source_transforms;
	for (auto &field : fields) {
		auto &name = field.GetPartitionSpecFieldName();
		// Generating another label must not turn a repeated expression into a second partition field.
		if (!source_transforms[field.source_id].insert(field.transform.RawType()).second) {
			throw InvalidInputException("Cannot use partition name more than once: %s", name);
		}
		reserved_names.insert(name);
		if (field.partition_field_id < first_new_field_id && !assigned_names.insert(name).second) {
			throw InvalidInputException("Cannot use partition name more than once: %s", name);
		}
	}

	for (auto &field : fields) {
		if (field.partition_field_id < first_new_field_id) {
			continue;
		}
		auto &name = field.GetPartitionSpecFieldName();
		auto column = schema_field_ids.find(name);
		bool schema_conflict =
		    column != schema_field_ids.end() && (field.transform.Type() != IcebergTransformType::IDENTITY ||
		                                         static_cast<uint64_t>(column->second) != field.source_id);
		if (schema_conflict || assigned_names.count(name)) {
			auto source_name = schema.GetFullColumnName(field.source_id);
			auto base_suffix = "_" + to_string(field.partition_field_id);
			auto suffix = base_suffix;
			idx_t attempt = 0;
			do {
				field.SetPartitionSpecFieldName(source_name, suffix);
				suffix = base_suffix + "_" + to_string(++attempt);
			} while (reserved_names.count(name));
		}
		assigned_names.insert(name);
		reserved_names.insert(name);
	}
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
