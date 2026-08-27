#include "planning/pruning/iceberg_file_pruner.hpp"

#include "core/expression/iceberg_predicate_stats.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/logging/logger.hpp"
#include "iceberg_logging.hpp"
#include "planning/pruning/iceberg_predicate.hpp"
#include "storage/statistics/iceberg_variant_statistics.hpp"

namespace duckdb {

namespace {

//! How many of a column's values in a data file are NULL, from the counts its manifest entry carries. An absent
//! null count tells us nothing, so assume both kinds of row are present.
void ApplyNullCounts(const IcebergDataFile &data_file, int32_t column_id, IcebergPredicateStats &stats) {
	optional<int64_t> value_count;
	optional<int64_t> null_count;
	auto value_counts_it = data_file.value_counts.find(column_id);
	if (value_counts_it != data_file.value_counts.end()) {
		value_count = value_counts_it->second;
	}
	auto null_counts_it = data_file.null_value_counts.find(column_id);
	if (null_counts_it != data_file.null_value_counts.end()) {
		null_count = null_counts_it->second;
	}

	if (null_count) {
		stats.has_null = *null_count > 0;
		stats.has_not_null = value_count ? *value_count - *null_count > 0 : true;
	} else {
		stats.has_null = true;
		stats.has_not_null = value_count ? *value_count > 0 : true;
	}
}

//! The stats a single partition value amounts to: a degenerate range whose ends are that one value.
IcebergPredicateStats PartitionValueStats(const IcebergDataFile &data_file, const ColumnIndex &column_index,
                                          const Value &partition_value) {
	IcebergPredicateStats stats;
	stats.lower_bound = partition_value;
	stats.upper_bound = partition_value;
	if (partition_value.IsNull()) {
		//! `void` yields NULL whatever the rows hold, so leave `has_not_null` at its conservative default.
		stats.has_null = true;
	} else {
		//! Transforms carry NULL through to NULL, so a non-NULL partition value rules NULL rows out.
		stats.has_null = false;
		stats.has_not_null = true;
	}
	//! Keyed by column index rather than field id, which is how this has always read the counts.
	auto nan_counts_it = data_file.nan_value_counts.find(column_index.GetPrimaryIndex());
	if (nan_counts_it != data_file.nan_value_counts.end()) {
		stats.has_nan = nan_counts_it->second != 0;
	}
	return stats;
}

//! Without the id, a name-mapped table cannot trust that a data file's id means this column.
bool MappingCoversFieldId(const IcebergTableMetadata &metadata, const unordered_set<int32_t> &mapping_field_ids,
                          int32_t field_id) {
	if (metadata.mappings.empty()) {
		return true;
	}
	return mapping_field_ids.find(field_id) != mapping_field_ids.end();
}

//! The bounds and null counts a data file records for one column, decoded. Empty when the manifest carries
//! nothing usable, in which case this column's bounds prove nothing either way.
optional<IcebergPredicateStats> TryGetColumnBoundStats(ClientContext &context, const IcebergTableMetadata &metadata,
                                                       const unordered_set<int32_t> &mapping_field_ids,
                                                       const IcebergDataFile &data_file,
                                                       const IcebergColumnDefinition &column) {
	if (data_file.lower_bounds.empty() || data_file.upper_bounds.empty() ||
	    data_file.content == IcebergManifestEntryContentType::POSITION_DELETES) {
		return {};
	}
	auto &column_id = column.id;
	if (!MappingCoversFieldId(metadata, mapping_field_ids, column_id)) {
		return {};
	}

	auto lower_bound_it = data_file.lower_bounds.find(column_id);
	auto upper_bound_it = data_file.upper_bounds.find(column_id);
	Value lower_bound;
	Value upper_bound;
	if (lower_bound_it != data_file.lower_bounds.end()) {
		lower_bound = lower_bound_it->second;
	}
	if (upper_bound_it != data_file.upper_bounds.end()) {
		upper_bound = upper_bound_it->second;
	}

	IcebergPredicateStats stats;
	if (column.type.id() == LogicalTypeId::VARIANT) {
		//! An encoded blob is only decodable when both ends are present.
		if (lower_bound.IsNull() || upper_bound.IsNull()) {
			return {};
		}
		Value lower_decoded;
		Value upper_decoded;
		Value lower_variant;
		Value upper_variant;
		auto lower_blob = lower_bound.GetValueUnsafe<string_t>();
		auto upper_blob = upper_bound.GetValueUnsafe<string_t>();
		if (IcebergVariantBoundsReader::Deserialize(context, lower_blob, lower_decoded) &&
		    IcebergVariantBoundsReader::RekeyBoundsVariant(lower_decoded, lower_variant)) {
			stats.SetLowerBound(lower_variant);
		}
		if (IcebergVariantBoundsReader::Deserialize(context, upper_blob, upper_decoded) &&
		    IcebergVariantBoundsReader::RekeyBoundsVariant(upper_decoded, upper_variant)) {
			stats.SetUpperBound(upper_variant);
		}
	} else {
		stats = IcebergPredicateStats::DeserializeBounds(lower_bound, upper_bound, column.name, column.type);
	}

	ApplyNullCounts(data_file, column_id, stats);

	auto nan_counts_it = data_file.nan_value_counts.find(column_id);
	stats.has_nan = nan_counts_it == data_file.nan_value_counts.end() || nan_counts_it->second > 0;
	return stats;
}

void LogPartitionPruned(ClientContext &context, const IcebergTableSchema &schema, const IcebergDataFile &data_file,
                        const ColumnIndex &column_index, const IcebergPartitionSpecField &field,
                        const IcebergPredicateStats &stats, const ExpressionFilter &filter) {
	auto &source_column = IcebergTableSchema::GetFromColumnIndex(schema.columns, column_index, 0);
	auto partition_value_raw_str = stats.lower_bound ? stats.lower_bound->ToString() : "NULL";
	auto partition_value_transformed_str =
	    stats.lower_bound ? field.transform.PartitionValueToString(*stats.lower_bound) : "NULL";
	DUCKDB_LOG(context, IcebergLogType,
	           "Iceberg Filter Pushdown, skipped 'data_file': '%s', partition column '%s' has raw value %s "
	           "with transform '%s'. '%s(%s)=%s' does not match filter: %s",
	           data_file.file_path, source_column.name, partition_value_raw_str, field.transform.RawType(),
	           field.transform.RawType(), partition_value_raw_str, partition_value_transformed_str,
	           filter.ToString(source_column.name));
}

void LogBoundsPruned(ClientContext &context, const IcebergDataFile &data_file, const IcebergColumnDefinition &column,
                     const IcebergPredicateStats &stats, const ExpressionFilter &filter) {
	DUCKDB_LOG(context, IcebergLogType,
	           "Iceberg Filter Pushdown, skipped 'data_file': '%s', column '%s' with "
	           "bounds [%s, %s] did not match filter: %s",
	           data_file.file_path, column.name, stats.lower_bound ? stats.lower_bound->ToString() : "N/A",
	           stats.upper_bound ? stats.upper_bound->ToString() : "N/A", filter.ToString(column.name));
}

} // namespace

//! The spec lists fields and the file lists values, neither keyed by column, so pair them up once here
//! instead of rescanning both for every filter.
column_index_map<vector<IcebergFilePruner::PartitionFieldValue>>
IcebergFilePruner::PartitionValuesByFilterColumn(const IcebergDataFile &data_file,
                                                 const IcebergManifestFile &manifest_file) const {
	column_index_map<vector<PartitionFieldValue>> result;
	if (data_file.partition_info.empty()) {
		return result;
	}

	auto partition_spec_it = metadata.partition_specs.find(manifest_file.partition_spec_id);
	if (partition_spec_it == metadata.partition_specs.end()) {
		throw InvalidConfigurationException(
		    "Data file %s has partition spec %d while the metadata does not have this partition spec",
		    data_file.file_path, manifest_file.partition_spec_id);
	}
	auto &partition_spec = partition_spec_it->second;
	auto &source_to_column_id = schema.GetSourceIdMap();

	unordered_map<uint64_t, idx_t> partition_info_map;
	for (idx_t i = 0; i < data_file.partition_info.size(); i++) {
		partition_info_map.emplace(data_file.partition_info[i].field_id, i);
	}

	for (auto &field : partition_spec.fields) {
		auto source_it = source_to_column_id.find(field.source_id);
		if (source_it == source_to_column_id.end()) {
			continue;
		}
		auto &column_index = source_it->second;

		//! A filter on a nested field is registered against its top-level column, so index the evidence
		//! under the key the caller's filter loop will iterate rather than the field's own column.
		auto owner_key = column_index;
		if (!table_filters.TryGetFilterByColumnIndex(owner_key) && owner_key.HasChildren()) {
			owner_key = ColumnIndex(owner_key.GetPrimaryIndex());
		}
		if (!table_filters.TryGetFilterByColumnIndex(owner_key)) {
			continue;
		}
		auto value_it = partition_info_map.find(field.partition_field_id);
		if (value_it == partition_info_map.end()) {
			continue;
		}
		result[owner_key].push_back({field, data_file.partition_info[value_it->second].value, column_index});
	}
	return result;
}

//! Decides whether a data file has to be read at all: one filter ruling it out settles that on its own,
//! while proving every row matches takes all of them.
METADATA_STATS_PUSHDOWN IcebergFilePruner::FileMatchesFilter(const IcebergManifestFile &manifest_file,
                                                             const IcebergManifestEntry &manifest_entry) const {
	D_ASSERT(table_filters.HasFilters());
	if (!table_filters.HasFilters()) {
		//! Nothing pushed down proves nothing; reporting coverage would drop an unselected file.
		return METADATA_STATS_PUSHDOWN::SOME_ROWS_MATCH;
	}

	auto &data_file = manifest_entry.data_file;
	unordered_set<int32_t> mapping_field_ids;
	for (auto &mapping : metadata.mappings) {
		if (mapping.field_id != NumericLimits<int32_t>::Maximum()) {
			mapping_field_ids.insert(mapping.field_id);
		}
	}

	auto partition_values = PartitionValuesByFilterColumn(data_file, manifest_file);
	//! The file is covered only if every filter is. A filter no evidence settles leaves it at SOME.
	bool all_filters_covered = true;

	for (auto &entry : table_filters) {
		auto &column_index = entry.first;
		auto &filter = *entry.second;
		auto &column = *schema.columns[column_index.GetPrimaryIndex()];
		bool filter_covered = false;

		//! Evidence from the partition values: any one field proving the filter is enough, so all are tried.
		auto partition_it = partition_values.find(column_index);
		if (partition_it != partition_values.end()) {
			for (auto &partition : partition_it->second) {
				auto &field = partition.field.get();
				//! Resolved from the field's own column, so a nested source yields only the part of the
				//! parent filter targeting its path rather than the parent's whole predicate.
				auto partition_filter = table_filters.GetFilterForColumnIndex(partition.source_column);
				if (!partition_filter) {
					continue;
				}
				auto stats = PartitionValueStats(data_file, column_index, partition.value.get());
				switch (IcebergPredicate::MatchBounds(context, *partition_filter, stats, field.transform)) {
				case METADATA_STATS_PUSHDOWN::NO_ROWS_MATCH:
					LogPartitionPruned(context, schema, data_file, column_index, field, stats, *partition_filter);
					return METADATA_STATS_PUSHDOWN::NO_ROWS_MATCH;
				case METADATA_STATS_PUSHDOWN::ALL_ROWS_MATCH:
					filter_covered = true;
					break;
				case METADATA_STATS_PUSHDOWN::SOME_ROWS_MATCH:
					break;
				}
			}
		}

		//! Evidence from the file's own column bounds, exact for this file where the partition value is not.
		if (auto bound_stats = TryGetColumnBoundStats(context, metadata, mapping_field_ids, data_file, column)) {
			switch (IcebergPredicate::MatchBounds(context, filter, *bound_stats, IcebergTransform::Identity())) {
			case METADATA_STATS_PUSHDOWN::NO_ROWS_MATCH:
				LogBoundsPruned(context, data_file, column, *bound_stats, filter);
				return METADATA_STATS_PUSHDOWN::NO_ROWS_MATCH;
			case METADATA_STATS_PUSHDOWN::ALL_ROWS_MATCH:
				filter_covered = true;
				break;
			case METADATA_STATS_PUSHDOWN::SOME_ROWS_MATCH:
				break;
			}
		}

		if (!filter_covered) {
			all_filters_covered = false;
		}
	}

	return all_filters_covered ? METADATA_STATS_PUSHDOWN::ALL_ROWS_MATCH : METADATA_STATS_PUSHDOWN::SOME_ROWS_MATCH;
}

bool IcebergFilePruner::DeleteManifestMatchesDataFile(const IcebergManifestFile &delete_manifest,
                                                      const IcebergManifestFile &data_manifest,
                                                      const IcebergManifestEntry &data_manifest_entry) const {
	if (!delete_manifest.sequence_number) {
		throw InvalidConfigurationException("Delete manifest %s does not have a sequence number",
		                                    delete_manifest.manifest_path);
	}
	if (*delete_manifest.sequence_number < data_manifest_entry.GetSequenceNumber(data_manifest)) {
		return false;
	}

	auto partition_spec_it = metadata.partition_specs.find(delete_manifest.partition_spec_id);
	if (partition_spec_it == metadata.partition_specs.end()) {
		throw InvalidInputException("Delete manifest %s references partition_spec_id %d which doesn't exist",
		                            delete_manifest.manifest_path, delete_manifest.partition_spec_id);
	}
	auto &delete_partition_spec = partition_spec_it->second;
	if (delete_partition_spec.IsUnpartitioned()) {
		return true;
	}
	if (delete_manifest.partition_spec_id != data_manifest.partition_spec_id) {
		return false;
	}
	if (!delete_manifest.partitions.has_partitions) {
		//! NOTE: This is conservative: the manifest doesn't have partition stats, but the partition spec matches
		//! So the manifest entries in the delete manifest might still apply
		return true;
	}

	auto &field_summaries = delete_manifest.partitions.field_summary;
	if (delete_partition_spec.fields.size() != field_summaries.size()) {
		throw InvalidInputException("Delete manifest has %d partition summaries but partition spec %d has %d fields",
		                            field_summaries.size(), delete_manifest.partition_spec_id,
		                            delete_partition_spec.fields.size());
	}

	unordered_map<uint64_t, reference<const Value>> partition_values;
	for (auto &partition : data_manifest_entry.data_file.partition_info) {
		partition_values.emplace(partition.field_id, partition.value);
	}

	for (idx_t field_idx = 0; field_idx < delete_partition_spec.fields.size(); field_idx++) {
		auto &field = delete_partition_spec.fields[field_idx];
		auto partition_value_it = partition_values.find(field.partition_field_id);
		if (partition_value_it == partition_values.end()) {
			return true;
		}
		auto &partition_value = partition_value_it->second.get();
		auto &field_summary = field_summaries[field_idx];
		if (partition_value.IsNull()) {
			if (!field_summary.contains_null) {
				return false;
			}
			continue;
		}

		auto source_column = metadata.FindColumnByFieldId(NumericCast<int32_t>(field.source_id));
		if (!source_column) {
			return true;
		}
		auto partition_type = field.transform.GetSerializedType(source_column->type);
		auto stats = IcebergPredicateStats::DeserializeBounds(field_summary.lower_bound, field_summary.upper_bound,
		                                                      source_column->name, partition_type);
		auto typed_partition_value = partition_value.DefaultCastAs(partition_type);
		if (stats.lower_bound && typed_partition_value < *stats.lower_bound) {
			return false;
		}
		if (stats.upper_bound && typed_partition_value > *stats.upper_bound) {
			return false;
		}
	}
	return true;
}

bool IcebergFilePruner::EqualityDeleteMatchesDataFile(const IcebergDataFile &delete_file,
                                                      const IcebergDataFile &data_file) const {
	auto &equality_ids = delete_file.equality_ids;

	for (auto field_id : equality_ids) {
		auto delete_null_count = delete_file.null_value_counts.find(field_id);
		if (delete_null_count == delete_file.null_value_counts.end() || delete_null_count->second != 0) {
			//! A NULL delete key can match NULL data values - require a known zero null count
			continue;
		}

		auto delete_lower = delete_file.lower_bounds.find(field_id);
		auto delete_upper = delete_file.upper_bounds.find(field_id);
		auto data_lower = data_file.lower_bounds.find(field_id);
		auto data_upper = data_file.upper_bounds.find(field_id);
		if (delete_lower == delete_file.lower_bounds.end() || delete_upper == delete_file.upper_bounds.end() ||
		    data_lower == data_file.lower_bounds.end() || data_upper == data_file.upper_bounds.end()) {
			continue;
		}

		auto column_p = metadata.FindColumnByFieldId(field_id);
		if (!column_p) {
			//! Could not locate the column in the current or any historical schema
			continue;
		}
		auto &column = *column_p;
		if (column.type.id() == LogicalTypeId::FLOAT || column.type.id() == LogicalTypeId::DOUBLE) {
			auto delete_nan_count = delete_file.nan_value_counts.find(field_id);
			if (delete_nan_count == delete_file.nan_value_counts.end() || delete_nan_count->second != 0) {
				//! Manifest bounds exclude NaNs - require a known zero NaN count
				continue;
			}
		}

		try {
			auto delete_stats = IcebergPredicateStats::DeserializeBounds(delete_lower->second, delete_upper->second,
			                                                             column.name, column.type);
			auto data_stats = IcebergPredicateStats::DeserializeBounds(data_lower->second, data_upper->second,
			                                                           column.name, column.type);
			if (!delete_stats.lower_bound || !delete_stats.upper_bound || !data_stats.lower_bound ||
			    !data_stats.upper_bound || delete_stats.lower_bound->IsNull() || delete_stats.upper_bound->IsNull() ||
			    data_stats.lower_bound->IsNull() || data_stats.upper_bound->IsNull()) {
				continue;
			}
			//! Test for either of these conditions:
			//! data:                   L --------- U
			//! delete:                               L --------- U
			//! delete:   L --------- U
			if (*delete_stats.upper_bound < *data_stats.lower_bound ||
			    *delete_stats.lower_bound > *data_stats.upper_bound) {
				DUCKDB_LOG(context, IcebergLogType,
				           "Iceberg Equality Delete Pruning, skipped 'equality_delete_file': '%s' for 'data_file': "
				           "'%s', equality field '%s' (field id %d) has bounds [%s, %s] outside data bounds [%s, %s]",
				           delete_file.file_path, data_file.file_path, column.name, field_id,
				           delete_stats.lower_bound->ToString(), delete_stats.upper_bound->ToString(),
				           data_stats.lower_bound->ToString(), data_stats.upper_bound->ToString());
				return false;
			}
		} catch (std::exception &e) {
			ErrorData error(e);
			DUCKDB_LOG_DEBUG(context,
			                 "Bounds for data file / equality delete file failed to deserialize in "
			                 "EqualityDeleteMatchesDataFile, ignoring error: %s",
			                 error.Message());
			continue;
		}
	}
	return true;
}

partition_value_map_t IcebergFilePruner::PartitionValueMap(const IcebergDataFile &data_file) {
	partition_value_map_t result;
	for (auto &partition : data_file.partition_info) {
		result.emplace(partition.field_id, partition.value);
	}
	return result;
}

bool IcebergFilePruner::DeleteFileMatchesDataFile(const IcebergManifestFile &delete_manifest,
                                                  const IcebergManifestEntry &delete_manifest_entry,
                                                  const IcebergManifestFile &data_manifest,
                                                  const IcebergManifestEntry &data_manifest_entry,
                                                  const partition_value_map_t &data_partition_values) const {
	auto &delete_file = delete_manifest_entry.data_file;
	auto &data_file = data_manifest_entry.data_file;
	if (delete_file.referenced_data_file && *delete_file.referenced_data_file != data_file.file_path) {
		return false;
	}

	auto delete_sequence_number = delete_manifest_entry.GetSequenceNumber(delete_manifest);
	auto data_sequence_number = data_manifest_entry.GetSequenceNumber(data_manifest);

	switch (delete_file.content) {
	case IcebergManifestEntryContentType::EQUALITY_DELETES: {
		if (delete_sequence_number <= data_sequence_number) {
			return false;
		}
		break;
	}
	case IcebergManifestEntryContentType::POSITION_DELETES: {
		if (delete_sequence_number < data_sequence_number) {
			return false;
		}
		break;
	}
	default:
		throw InternalException("Unexpected manifest entry content type: %d",
		                        static_cast<uint8_t>(delete_file.content));
	}

	auto partition_spec_it = metadata.partition_specs.find(delete_manifest.partition_spec_id);
	if (partition_spec_it == metadata.partition_specs.end()) {
		throw InvalidInputException("Delete manifest %s references partition_spec_id %d which doesn't exist",
		                            delete_manifest.manifest_path, delete_manifest.partition_spec_id);
	}
	if (!partition_spec_it->second.IsUnpartitioned()) {
		if (delete_manifest.partition_spec_id != data_manifest.partition_spec_id) {
			return false;
		}

		//! Both files are under this spec, so its fields are the partition.
		for (auto &field : partition_spec_it->second.fields) {
			const Value *delete_value = nullptr;
			for (auto &delete_partition : delete_file.partition_info) {
				if (delete_partition.field_id == field.partition_field_id) {
					delete_value = &delete_partition.value;
					break;
				}
			}
			const Value *data_value = nullptr;
			auto data_it = data_partition_values.find(field.partition_field_id);
			if (data_it != data_partition_values.end()) {
				data_value = &data_it->second.get();
			}
			if (!delete_value || !data_value) {
				return false;
			}
			if (!Value::NotDistinctFrom(*delete_value, *data_value)) {
				return false;
			}
		}
	}
	if (delete_file.content == IcebergManifestEntryContentType::EQUALITY_DELETES) {
		return EqualityDeleteMatchesDataFile(delete_file, data_file);
	}
	return true;
}

bool IcebergFilePruner::ManifestMatchesFilter(const IcebergManifestFile &manifest) const {
	auto spec_id = manifest.partition_spec_id;
	auto partition_spec_it = metadata.partition_specs.find(spec_id);
	if (partition_spec_it == metadata.partition_specs.end()) {
		throw InvalidInputException("Manifest %s references 'partition_spec_id' %d which doesn't exist",
		                            manifest.manifest_path, spec_id);
	}
	auto &partition_spec = partition_spec_it->second;
	if (!manifest.partitions.has_partitions) {
		return true;
	}

	auto &field_summaries = manifest.partitions.field_summary;
	if (partition_spec.fields.size() != field_summaries.size()) {
		throw InvalidInputException(
		    "Manifest has %d 'field_summary' entries but the referenced partition spec has %d fields",
		    field_summaries.size(), partition_spec.fields.size());
	}
	if (!table_filters.HasFilters()) {
		return true;
	}

	auto &source_to_column_id = schema.GetSourceIdMap();
	for (idx_t i = 0; i < field_summaries.size(); i++) {
		auto &field_summary = field_summaries[i];
		auto &field = partition_spec.fields[i];
		const auto &column_id = source_to_column_id.at(field.source_id);
		auto table_filter = table_filters.GetFilterForColumnIndex(column_id);
		if (!table_filter) {
			continue;
		}

		auto &column = IcebergTableSchema::GetFromColumnIndex(schema.columns, column_id, 0);
		auto result_type = field.transform.GetSerializedType(column.type);
		auto stats = IcebergPredicateStats::DeserializeBounds(field_summary.lower_bound, field_summary.upper_bound,
		                                                      column.name, result_type);
		stats.has_nan = field_summary.contains_nan;
		stats.has_null = field_summary.contains_null;
		stats.has_not_null = true;

		if (IcebergPredicate::MatchBounds(context, *table_filter, stats, field.transform) ==
		    METADATA_STATS_PUSHDOWN::NO_ROWS_MATCH) {
			DUCKDB_LOG(context, IcebergLogType,
			           "Iceberg Filter Pushdown, skipped 'manifest_file': '%s', column '%s' with "
			           "transform '%s', bounds [%s, %s] did not match filter: %s",
			           manifest.manifest_path, column.name, field.transform.RawType(),
			           stats.lower_bound ? stats.lower_bound->ToString() : "N/A",
			           stats.upper_bound ? stats.upper_bound->ToString() : "N/A", table_filter->ToString(column.name));
			return false;
		}
	}
	return true;
}

} // namespace duckdb
