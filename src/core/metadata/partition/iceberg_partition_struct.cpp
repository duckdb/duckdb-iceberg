#include "core/metadata/partition/iceberg_partition_struct.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"

namespace duckdb {

vector<IcebergPartitionStructField> IcebergPartitionStruct::GetFields(const IcebergTableMetadata &metadata) {
	auto &schema = metadata.GetLatestSchema();

	//! Newest spec first, so the most recent field names are used
	vector<reference<const IcebergPartitionSpec>> specs;
	for (auto &entry : metadata.partition_specs) {
		specs.push_back(entry.second);
	}
	std::sort(specs.begin(), specs.end(),
	          [](const IcebergPartitionSpec &a, const IcebergPartitionSpec &b) { return a.spec_id > b.spec_id; });

	map<uint64_t, IcebergPartitionStructField> fields;
	for (auto &spec : specs) {
		for (auto &field : spec.get().fields) {
			//! Fields whose source column is no longer in the schema are left out
			auto column = schema.TryGetColumnByFieldId(field.source_id);
			if (!column) {
				continue;
			}
			auto type = field.transform.GetResultType(column->type);
			auto existing = fields.find(field.partition_field_id);
			if (existing == fields.end()) {
				fields.emplace(field.partition_field_id,
				               IcebergPartitionStructField {field.GetPartitionSpecFieldName(), type, field});
			} else if (existing->second.field.get().transform.Type() == IcebergTransformType::VOID &&
			           field.transform.Type() != IcebergTransformType::VOID) {
				//! Fields dropped from v1 tables become void, use the field from before it was dropped
				existing->second.field = field;
				existing->second.type = type;
			}
		}
	}

	vector<IcebergPartitionStructField> result;
	case_insensitive_set_t names;
	for (auto &entry : fields) {
		auto field = std::move(entry.second);
		//! Fields from different partition specs can share a name, but struct field names have to be unique.
		if (!names.insert(field.name).second) {
			field.name = StringUtil::Format("%s_%d", field.name, field.field.get().partition_field_id);
			names.insert(field.name);
		}
		result.push_back(std::move(field));
	}
	return result;
}

LogicalType IcebergPartitionStruct::GetType(const vector<IcebergPartitionStructField> &fields) {
	child_list_t<LogicalType> children;
	for (auto &field : fields) {
		children.emplace_back(field.name, field.type);
	}
	return LogicalType::STRUCT(std::move(children));
}

Value IcebergPartitionStruct::GetValue(const vector<IcebergPartitionStructField> &fields,
                                       const IcebergDataFile &data_file) {
	child_list_t<Value> children;
	for (auto &field : fields) {
		auto &spec_field = field.field.get();
		Value value(field.type);
		for (auto &info : data_file.partition_info) {
			if (info.field_id != spec_field.partition_field_id || info.value.IsNull()) {
				continue;
			}
			if (spec_field.transform.Type() == IcebergTransformType::DAY) {
				value = Value::DATE(date_t(info.value.GetValue<int32_t>()));
			} else {
				value = info.value.DefaultCastAs(field.type);
			}
			break;
		}
		children.emplace_back(field.name, std::move(value));
	}
	return Value::STRUCT(std::move(children));
}

} // namespace duckdb
