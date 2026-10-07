#include "core/metadata/partition/iceberg_partition_constants.hpp"
#include "core/expression/iceberg_value.hpp"

namespace duckdb {

optional_ptr<const LogicalType> IcebergPartitionConstants::GetType(int32_t field_id, const IcebergTableSchema &schema,
                                                                   const IcebergTableMetadataSchemas &schemas) {
	auto column = schema.TryGetColumnByFieldId(field_id);
	if (column) {
		return column->type;
	}
	// A dropped source can have several historical types. Iceberg promotions widen types, so use
	// the widest historical version to read partitions from all schemas without narrowing values.
	optional_ptr<const LogicalType> source_type;
	schemas.ForEachSchema([&](const IcebergTableSchema &historical_schema) {
		auto historical_column = historical_schema.TryGetColumnByFieldId(field_id);
		if (!historical_column) {
			return;
		}
		auto &historical_type = historical_column->type;
		if (!source_type) {
			source_type = historical_type;
			return;
		}
		LogicalType widest_type;
		if (LogicalType::DefaultTryGetMaxLogicalTypeUnchecked(*source_type, historical_type, widest_type) &&
		    widest_type == historical_type) {
			source_type = historical_type;
		}
	});
	return source_type;
}

unordered_map<int32_t, Value> IcebergPartitionConstants::Resolve(int32_t spec_id,
                                                                 const vector<IcebergPartitionInfo> &partition_values,
                                                                 const IcebergTableMetadata &metadata,
                                                                 const IcebergTableSchema &schema) {
	auto spec = metadata.partition_specs.find(spec_id);
	if (spec == metadata.partition_specs.end()) {
		throw InvalidConfigurationException("'partition_spec_id' %d doesn't exist in the metadata", spec_id);
	}
	unordered_map<int32_t, Value> constants;
	//! A source column can have several partition fields (e.g. identity and bucket); only identity ones carry its value
	for (auto &field : spec->second.fields) {
		if (field.transform != IcebergTransformType::IDENTITY || constants.count(field.source_id)) {
			continue;
		}
		auto type = GetType(field.source_id, schema, metadata.GetSchemas());
		if (!type) {
			continue;
		}
		for (auto &partition : partition_values) {
			if (partition.field_id == field.partition_field_id && !partition.value.IsNull()) {
				constants.emplace(field.source_id, IcebergValue::TransformPartitionValue(partition.value, *type));
				break;
			}
		}
	}
	return constants;
}

} // namespace duckdb
