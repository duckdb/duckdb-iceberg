#include "planning/scan_plan/iceberg_scan_statistics_cache.hpp"

#include "core/expression/iceberg_predicate_stats.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

//! Types whose Iceberg bound ordering matches DuckDB's. Floats lack NaN handling, strings and binary may be
//! truncated, and UUID ordering has not been verified against all writers.
static bool SupportsBounds(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return true;
	default:
		return false;
	}
}

struct IcebergColumnAggregate {
	explicit IcebergColumnAggregate(const IcebergColumnDefinition &column_p) : column(column_p) {
	}

	const IcebergColumnDefinition &column;
	idx_t file_count = 0;
	bool validity_complete = true;
	bool saw_null = false;
	bool saw_valid = false;
	bool lower_complete = true;
	bool upper_complete = true;
	optional<Value> lower_bound;
	optional<Value> upper_bound;

	static optional<Value> PromoteBound(const optional<Value> &bound, const LogicalType &target_type) {
		if (!bound || bound->IsNull()) {
			return nullopt;
		}
		string error;
		auto promoted = bound->DefaultTryCastAs(target_type, &error, true);
		if (!promoted) {
			return nullopt;
		}
		return std::move(*promoted);
	}

	//! writer_column is the column as the manifest's writer schema defines it, or nullptr when that schema is unknown
	void AddFile(ClientContext &context, const IcebergDataFile &file,
	             optional_ptr<const IcebergColumnDefinition> writer_column, optional_ptr<const Value> identity_value) {
		if (file.record_count == 0) {
			return;
		}
		file_count++;

		bool counts_known = false;
		bool file_has_null = false;
		bool file_has_valid = false;
		if (identity_value) {
			counts_known = true;
			file_has_null = identity_value->IsNull();
			file_has_valid = !identity_value->IsNull();
		} else {
			auto value_count = file.value_counts.find(column.id);
			auto null_count = file.null_value_counts.find(column.id);
			if (value_count != file.value_counts.end() && null_count != file.null_value_counts.end() &&
			    value_count->second >= 0 && null_count->second >= 0 && value_count->second >= null_count->second) {
				counts_known = true;
				file_has_null = null_count->second > 0;
				file_has_valid = value_count->second > null_count->second;
			}
		}
		if (!counts_known) {
			validity_complete = false;
			// Without complete counts this file may contain non-NULL values, so it must contribute bounds.
			file_has_valid = true;
		}
		saw_null |= file_has_null;
		saw_valid |= file_has_valid;

		if (!file_has_valid || !SupportsBounds(column.type)) {
			return;
		}

		optional<Value> file_lower;
		optional<Value> file_upper;
		if (identity_value && !identity_value->IsNull()) {
			file_lower = PromoteBound(optional<Value>(*identity_value), column.type);
			file_upper = PromoteBound(optional<Value>(*identity_value), column.type);
		} else if (writer_column) {
			auto lower = file.lower_bounds.find(column.id);
			auto upper = file.upper_bounds.find(column.id);
			Value lower_value;
			Value upper_value;
			if (lower != file.lower_bounds.end()) {
				lower_value = lower->second;
			}
			if (upper != file.upper_bounds.end()) {
				upper_value = upper->second;
			}
			auto decoded = IcebergPredicateStats::DeserializeBounds(context, lower_value, upper_value, column.name,
			                                                        writer_column->type);
			file_lower = PromoteBound(decoded.lower_bound, column.type);
			file_upper = PromoteBound(decoded.upper_bound, column.type);
		}

		if (!file_lower) {
			lower_complete = false;
		} else if (!lower_bound || *file_lower < *lower_bound) {
			lower_bound = std::move(*file_lower);
		}
		if (!file_upper) {
			upper_complete = false;
		} else if (!upper_bound || *file_upper > *upper_bound) {
			upper_bound = std::move(*file_upper);
		}
	}

	unique_ptr<BaseStatistics> Finalize() const {
		if (file_count == 0) {
			return BaseStatistics::CreateEmpty(column.type).ToUnique();
		}

		auto result = BaseStatistics::CreateUnknown(column.type);
		bool useful = false;
		if (validity_complete && !saw_null) {
			result.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
			useful = true;
		}
		if (validity_complete && !saw_valid) {
			result.Set(StatsInfo::CANNOT_HAVE_VALID_VALUES);
			useful = true;
		}
		if (saw_valid && lower_complete && upper_complete && lower_bound && upper_bound &&
		    BaseStatistics::GetStatsType(column.type) == StatisticsType::NUMERIC_STATS) {
			NumericStats::SetMin(result, *lower_bound);
			NumericStats::SetMax(result, *upper_bound);
			useful = true;
		}
		return useful ? result.ToUnique() : nullptr;
	}
};

//! The schema each data manifest was written with: the selected schema when the manifest names none, or nullptr
//! when it names a schema the table metadata does not contain
static vector<optional_ptr<const IcebergTableSchema>>
GetWriterSchemas(ClientContext &context, const IcebergTableMetadata &metadata, const IcebergTableSchema &schema,
                 const vector<BoundIcebergManifestListEntry> &data_manifests) {
	vector<optional_ptr<const IcebergTableSchema>> result;
	result.reserve(data_manifests.size());
	for (auto &manifest : data_manifests) {
		auto &manifest_metadata = manifest.entry.manifest_metadata;
		if (!manifest_metadata) {
			result.push_back(&schema);
			continue;
		}
		auto writer_schema = metadata.GetSchemas().TryGetSchemaFromId(manifest_metadata->schema_id);
		if (!writer_schema) {
			DUCKDB_LOG(context, IcebergLogType,
			           "Omitting manifest bounds from statistics: writer schema id %d not found in table metadata",
			           manifest_metadata->schema_id);
		}
		result.push_back(writer_schema);
	}
	return result;
}

static optional_ptr<const IcebergColumnDefinition> GetWriterColumn(optional_ptr<const IcebergTableSchema> writer_schema,
                                                                   const IcebergColumnDefinition &selected_column) {
	if (!writer_schema) {
		return nullptr;
	}
	auto writer_column = writer_schema->TryGetColumnByFieldId(selected_column.id);
	if (writer_column) {
		return writer_column;
	}
	// A column added after the manifest was written has no bounds in it, so the selected definition is safe
	return &selected_column;
}

//! Maps every identity partition field id, across all partition specs, to its source field id
static unordered_map<uint64_t, int32_t> GetIdentitySourceIds(const IcebergTableMetadata &metadata) {
	unordered_map<uint64_t, int32_t> result;
	for (auto &spec_entry : metadata.partition_specs) {
		for (auto &field : spec_entry.second.fields) {
			if (field.transform.Type() != IcebergTransformType::IDENTITY ||
			    field.source_id > static_cast<uint64_t>(NumericLimits<int32_t>::Maximum())) {
				continue;
			}
			result.emplace(field.partition_field_id, static_cast<int32_t>(field.source_id));
		}
	}
	return result;
}

struct IcebergScanStatisticsCache::State {
	struct PartitionRowGroupData : public PartitionRowGroup {
		PartitionRowGroupData(State &state_p, idx_t file_index_p) : state(state_p), file_index(file_index_p) {
		}

		unique_ptr<BaseStatistics> GetColumnStatistics(const StorageIndex &storage_index) override {
			if (storage_index.IsPushdownExtract()) {
				return nullptr;
			}
			return state.GetFileColumnStatistics(file_index, storage_index.GetPrimaryIndex());
		}

		bool MinMaxIsExact(const StorageIndex &) override {
			// Manifest bounds only enclose values, so they cannot answer MIN/MAX or position rows for OFFSET pruning
			return false;
		}

		bool HasPendingWrites() override {
			return state.has_pending_writes;
		}

		State &state;
		idx_t file_index;
	};

	State(ClientContext &context_p, const IcebergTableMetadata &metadata_p, const IcebergTableSchema &schema_p,
	      const vector<BoundIcebergManifestListEntry> &data_manifests_p,
	      const vector<BoundIcebergManifestEntry> &data_files_p, bool has_pending_writes_p)
	    : context(context_p), schema(schema_p), data_files(data_files_p),
	      writer_schemas(GetWriterSchemas(context_p, metadata_p, schema_p, data_manifests_p)),
	      identity_source_ids(GetIdentitySourceIds(metadata_p)), has_pending_writes(has_pending_writes_p) {
		global_columns.resize(schema.columns.size());
		global_columns_initialized.resize(schema.columns.size(), false);
		file_columns.resize(schema.columns.size());
		file_columns_initialized.resize(schema.columns.size(), false);
		file_counts.reserve(data_files.size());

		for (auto &bound_entry : data_files) {
			auto &file = bound_entry.entry.data_file;
			idx_t file_count = 0;
			if (file.record_count < 0) {
				DUCKDB_LOG(context, IcebergLogType,
				           "Omitting row counts from statistics: data file '%s' has a negative record count",
				           file.file_path);
				counts_valid = false;
			} else {
				file_count = NumericCast<idx_t>(file.record_count);
				if (file_count > NumericLimits<idx_t>::Maximum() - cardinality) {
					DUCKDB_LOG(context, IcebergLogType,
					           "Omitting row counts from statistics: selected data-file counts overflow");
					counts_valid = false;
				} else {
					cardinality += file_count;
				}
			}
			file_counts.push_back(file_count);
		}
	}

	optional_ptr<const Value> GetIdentityValue(const IcebergDataFile &file, int32_t source_field_id) const {
		for (auto &partition_info : file.partition_info) {
			auto source_id = identity_source_ids.find(partition_info.field_id);
			if (source_id != identity_source_ids.end() && source_id->second == source_field_id) {
				return partition_info.value;
			}
		}
		return nullptr;
	}

	optional_ptr<const IcebergColumnDefinition> GetWriterColumn(const BoundIcebergManifestEntry &entry,
	                                                            const IcebergColumnDefinition &selected_column) const {
		if (entry.manifest_file_idx >= writer_schemas.size()) {
			return nullptr;
		}
		return duckdb::GetWriterColumn(writer_schemas[entry.manifest_file_idx], selected_column);
	}

	void AddFile(IcebergColumnAggregate &aggregate, idx_t file_index) const {
		auto &entry = data_files[file_index];
		auto &file = entry.entry.data_file;
		auto writer_column = GetWriterColumn(entry, aggregate.column);
		auto identity_value = GetIdentityValue(file, aggregate.column.id);
		aggregate.AddFile(context, file, writer_column, identity_value);
	}

	unique_ptr<BaseStatistics> GetGlobalColumnStatistics(column_t column_index) {
		lock_guard<mutex> guard(statistics_lock);
		if (column_index >= global_columns.size() ||
		    BaseStatistics::GetStatsType(schema.columns[column_index]->type) != StatisticsType::NUMERIC_STATS) {
			return nullptr;
		}
		if (!global_columns_initialized[column_index]) {
			IcebergColumnAggregate aggregate(*schema.columns[column_index]);
			for (idx_t file_index = 0; file_index < data_files.size(); file_index++) {
				AddFile(aggregate, file_index);
			}
			global_columns[column_index] = aggregate.Finalize();
			global_columns_initialized[column_index] = true;
			DUCKDB_LOG(context, IcebergLogType, "Iceberg statistics phase=global_column_derived field_id=%d files=%llu",
			           schema.columns[column_index]->id, data_files.size());
		}
		return global_columns[column_index] ? global_columns[column_index]->ToUnique() : nullptr;
	}

	unique_ptr<BaseStatistics> GetFileColumnStatistics(idx_t file_index, column_t column_index) {
		lock_guard<mutex> guard(statistics_lock);
		if (file_index >= data_files.size() || column_index >= file_columns.size() ||
		    BaseStatistics::GetStatsType(schema.columns[column_index]->type) != StatisticsType::NUMERIC_STATS) {
			return nullptr;
		}
		if (!file_columns_initialized[column_index]) {
			auto &column_statistics = file_columns[column_index];
			column_statistics.resize(data_files.size());
			for (idx_t current_file = 0; current_file < data_files.size(); current_file++) {
				IcebergColumnAggregate aggregate(*schema.columns[column_index]);
				AddFile(aggregate, current_file);
				column_statistics[current_file] = aggregate.Finalize();
			}
			file_columns_initialized[column_index] = true;
			DUCKDB_LOG(context, IcebergLogType, "Iceberg statistics phase=file_column_derived field_id=%d files=%llu",
			           schema.columns[column_index]->id, data_files.size());
		}
		auto &result = file_columns[column_index][file_index];
		return result ? result->ToUnique() : nullptr;
	}

	ClientContext &context;
	const IcebergTableSchema &schema;
	//! Snapshot the lightweight entry handles so later scan-order changes cannot retarget lazy statistics.
	vector<BoundIcebergManifestEntry> data_files;
	vector<optional_ptr<const IcebergTableSchema>> writer_schemas;
	unordered_map<uint64_t, int32_t> identity_source_ids;
	bool has_pending_writes;
	bool counts_valid = true;
	idx_t cardinality = 0;
	vector<idx_t> file_counts;
	mutex statistics_lock;
	vector<bool> global_columns_initialized;
	vector<unique_ptr<BaseStatistics>> global_columns;
	vector<bool> file_columns_initialized;
	vector<vector<unique_ptr<BaseStatistics>>> file_columns;
};

IcebergScanStatisticsCache::IcebergScanStatisticsCache(ClientContext &context, const IcebergTableMetadata &metadata,
                                                       const IcebergTableSchema &schema,
                                                       const vector<BoundIcebergManifestListEntry> &data_manifests,
                                                       const vector<BoundIcebergManifestEntry> &data_files,
                                                       bool has_pending_writes)
    : state(make_uniq<State>(context, metadata, schema, data_manifests, data_files, has_pending_writes)) {
}

IcebergScanStatisticsCache::~IcebergScanStatisticsCache() {
}

optional<idx_t> IcebergScanStatisticsCache::Cardinality() const {
	if (!state->counts_valid) {
		return nullopt;
	}
	return state->cardinality;
}

unique_ptr<BaseStatistics> IcebergScanStatisticsCache::GetColumnStatistics(column_t column_index) const {
	return state->GetGlobalColumnStatistics(column_index);
}

void IcebergScanStatisticsCache::GetPartitionStatistics(vector<PartitionStatistics> &result) const {
	if (!state->counts_valid) {
		return;
	}
	idx_t row_start = 0;
	for (idx_t file_index = 0; file_index < state->data_files.size(); file_index++) {
		PartitionStatistics statistics;
		statistics.row_start = row_start;
		statistics.count = state->file_counts[file_index];
		statistics.count_type = CountType::COUNT_EXACT;
		statistics.partition_row_group = make_shared_ptr<State::PartitionRowGroupData>(*state, file_index);
		result.push_back(std::move(statistics));
		row_start += state->file_counts[file_index];
	}
}

} // namespace duckdb
